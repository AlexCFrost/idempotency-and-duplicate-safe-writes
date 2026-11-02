const { db } = require('./db');
const crypto = require('crypto');

const OPERATION = 'POST:/incidents';
const TTL_INTERVAL = `interval '24 hours'`;

function canonicalize(value) {
  if (Array.isArray(value)) return value.map(canonicalize);
  if (value && typeof value === 'object') {
    return Object.keys(value).sort().reduce((result, key) => {
      result[key] = canonicalize(value[key]);
      return result;
    }, {});
  }
  return value;
}

function hashRequest(body) {
  const canonical = canonicalize({
    title: body && body.title,
    severity: body && body.severity,
    serviceId: body && body.serviceId
  });
  return crypto.createHash('sha256').update(JSON.stringify(canonical)).digest('hex');
}

async function createIncident(req, res) {
  const key = req.get('Idempotency-Key');
  if (!key || !key.trim()) {
    return res.status(400).json({ error: 'idempotency_key_required' });
  }

  const { title, severity, serviceId } = req.body || {};
  if (!title || !severity || !serviceId) {
    return res.status(400).json({ error: 'invalid_request' });
  }
  if (!['P1', 'P2', 'P3', 'P4'].includes(severity)) {
    return res.status(400).json({ error: 'invalid_request' });
  }

  const requestHash = hashRequest({ title, severity, serviceId });
  const tenantId = req.user.tenantId;

  try {
    const result = await db.tx(async t => {
      const claimed = await t.oneOrNone(
        `INSERT INTO idempotency_keys
           (tenant_id, operation, key, request_hash, state, expires_at)
         VALUES ($1, $2, $3, $4, 'processing', now() + ${TTL_INTERVAL})
         ON CONFLICT (tenant_id, operation, key) DO NOTHING
         RETURNING tenant_id, operation, key, request_hash, state,
                   response_status, response_body, expires_at`,
        [tenantId, OPERATION, key.trim(), requestHash]
      );

      let record = claimed;
      if (!record) {
        record = await t.one(
          `SELECT tenant_id, operation, key, request_hash, state,
                  response_status, response_body, expires_at
           FROM idempotency_keys
           WHERE tenant_id = $1 AND operation = $2 AND key = $3`,
          [tenantId, OPERATION, key.trim()]
        );
      }

      const expired = record.expires_at && new Date(record.expires_at).getTime() <= Date.now();
      if (!claimed && expired) {
        record = await t.one(
          `UPDATE idempotency_keys
           SET request_hash = $4, state = 'processing', response_status = NULL,
               response_body = NULL, expires_at = now() + ${TTL_INTERVAL},
               updated_at = now()
           WHERE tenant_id = $1 AND operation = $2 AND key = $3
           RETURNING tenant_id, operation, key, request_hash, state,
                     response_status, response_body, expires_at`,
          [tenantId, OPERATION, key.trim(), requestHash]
        );
        claimed = record;
      }

      if (!claimed) {
        if (record.request_hash !== requestHash) {
          return { status: 409, body: { error: 'idempotency_key_conflict' } };
        }
        if (record.state === 'completed') {
          return {
            status: record.response_status,
            body: record.response_body,
            replayed: true
          };
        }
        if (record.state === 'processing') {
          return { status: 409, body: { error: 'operation_in_progress' } };
        }
        if (record.state === 'failed') {
          return { status: 409, body: { error: 'prior_operation_failed' } };
        }
        return { status: 409, body: { error: 'operation_in_progress' } };
      }

      const incident = await t.one(
        `INSERT INTO incidents (tenant_id, service_id, title, severity)
         VALUES ($1, $2, $3, $4)
         RETURNING *`,
        [tenantId, serviceId, title, severity]
      );

      await t.none(
        `INSERT INTO paging_jobs (incident_id, tenant_id, stable_id, job_type, state)
         VALUES ($1, $2, $3, 'page_incident', 'pending')
         ON CONFLICT (incident_id) DO NOTHING`,
        [incident.id, tenantId, incident.id]
      );

      const responseBody = {
        id: incident.id,
        tenant_id: incident.tenant_id,
        service_id: incident.service_id,
        title: incident.title,
        severity: incident.severity,
        created_at: incident.created_at
      };

      await t.none(
        `UPDATE idempotency_keys
         SET state = 'completed', response_status = 201,
             response_body = $4::jsonb, expires_at = now() + ${TTL_INTERVAL},
             updated_at = now()
         WHERE tenant_id = $1 AND operation = $2 AND key = $3`,
        [tenantId, OPERATION, key.trim(), JSON.stringify(responseBody)]
      );

      return { status: 201, body: responseBody, replayed: false };
    });

    if (result.replayed) res.set('Idempotent-Replayed', 'true');
    return res.status(result.status).json(result.body);
  } catch (error) {
    throw error;
  }
}

module.exports = { createIncident, hashRequest };
