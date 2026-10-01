// app.js — Satellite RMS browser client
// All fetch() calls use relative URLs (no hardcoded IP) because the page is
// served from the same Nginx origin as the API: http://<minikube-ip>/

'use strict';

// ─────────────────────────────────────────────────────────
// State
// ─────────────────────────────────────────────────────────

let token = localStorage.getItem('jwt');
let operator = localStorage.getItem('operator');   // "alice" or "bob"
let zone     = localStorage.getItem('zone');       // "GW-NORTH" / "GW-SOUTH"

// ─────────────────────────────────────────────────────────
// Startup — decide which screen to show
// ─────────────────────────────────────────────────────────

window.addEventListener('DOMContentLoaded', () => {
  document.getElementById('login-form').addEventListener('submit', login);

  if (token) {
    showDashboard();        // already logged in
    fetchSatellites();      // auto-load first tab
  }
});

// ─────────────────────────────────────────────────────────
// Auth
// ─────────────────────────────────────────────────────────

// login() is called by the form submit event.
// It posts credentials to /auth/login (Nginx routes this to auth-service:8081).
// On success, the JWT is stored in localStorage so it survives page refresh.
async function login(e) {
  e.preventDefault();
  const username = document.getElementById('username').value.trim();
  const password = document.getElementById('password').value;
  const errEl = document.getElementById('login-error');
  errEl.textContent = '';

  let res;
  try {
    // Relative URL — resolves to http://<minikube-ip>/auth/login
    // Nginx routes /auth/* to auth-service:8081
    res = await fetch('/auth/login', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ username, password }),
    });
  } catch (err) {
    errEl.textContent = 'Network error — is the cluster running?';
    return;
  }

  if (!res.ok) {
    const data = await res.json().catch(() => ({}));
    errEl.textContent = data.error || 'Invalid credentials';
    return;
  }

  const data = await res.json();
  // Store token and identity in localStorage for persistence across refreshes
  token    = data.token;
  operator = data.operator_id;
  zone     = data.gateway_zone;
  localStorage.setItem('jwt',      token);
  localStorage.setItem('operator', operator);
  localStorage.setItem('zone',     zone);

  showDashboard();
  fetchSatellites();
}

function logout() {
  localStorage.removeItem('jwt');
  localStorage.removeItem('operator');
  localStorage.removeItem('zone');
  token = operator = zone = null;
  showLogin();
}

// ─────────────────────────────────────────────────────────
// Screen switching
// ─────────────────────────────────────────────────────────

function showLogin() {
  document.getElementById('login-section').style.display    = 'flex';
  document.getElementById('dashboard-section').style.display = 'none';
}

function showDashboard() {
  document.getElementById('login-section').style.display    = 'none';
  document.getElementById('dashboard-section').style.display = 'block';

  document.getElementById('operator-label').textContent = `Operator: ${operator}`;

  const badge = document.getElementById('zone-badge');
  badge.textContent = zone;
  badge.className = 'badge ' + {
    'GW-NORTH':   'badge-north',
    'GW-SOUTH':   'badge-south',
    'GW-CENTRAL': 'badge-central',
  }[zone] || '';
}

function showTab(name, btn) {
  // Hide all tab content panels
  document.querySelectorAll('.tab-content').forEach(el => el.classList.remove('active'));
  document.querySelectorAll('.tab-btn').forEach(el => el.classList.remove('active'));
  document.getElementById('tab-' + name).classList.add('active');
  if (btn) btn.classList.add('active');

  // Lazy-load data when tab is first opened
  if (name === 'satellites') fetchSatellites();
  if (name === 'schedules')  fetchSchedules();
}

// ─────────────────────────────────────────────────────────
// API helpers
// ─────────────────────────────────────────────────────────

// apiGet sends an authenticated GET request.
// If the server returns 401 (expired/invalid token), the user is logged out.
async function apiGet(path) {
  const res = await fetch(path, {
    headers: { 'Authorization': 'Bearer ' + token },
  });
  if (res.status === 401) {
    logout();
    return null;
  }
  return res;
}

// apiPost sends an authenticated POST request with a JSON body.
async function apiPost(path, body) {
  const res = await fetch(path, {
    method: 'POST',
    headers: {
      'Authorization': 'Bearer ' + token,
      'Content-Type':  'application/json',
    },
    body: JSON.stringify(body),
  });
  if (res.status === 401) {
    logout();
    return null;
  }
  return res;
}

// ─────────────────────────────────────────────────────────
// Satellites tab
// ─────────────────────────────────────────────────────────

// fetchSatellites calls GET /api/satellites.
// Nginx routes /api/* to dashboard-api:8080.
// dashboard-api validates the JWT, filters satellites by zone, returns JSON.
async function fetchSatellites() {
  const container = document.getElementById('sat-container');
  const countEl   = document.getElementById('sat-count');
  container.innerHTML = '<p class="loading">Loading satellites…</p>';
  countEl.textContent = '';

  let res;
  try {
    res = await apiGet('/api/satellites');
  } catch (err) {
    container.innerHTML = '<p class="error-msg">Network error</p>';
    return;
  }
  if (!res) return;

  const data = await res.json();
  if (!res.ok) {
    container.innerHTML = `<p class="error-msg">${data.error}</p>`;
    return;
  }

  const sats = data.satellites || [];
  countEl.textContent = `${sats.length} satellites — ephemeris v${data.version} — ${data.timestamp}`;

  if (sats.length === 0) {
    container.innerHTML = '<p class="empty-msg">No satellites in your zone.</p>';
    return;
  }

  // Build a table — each row is one satellite.
  // Field names are snake_case because protoc-gen-go generates json tags from proto field names:
  //   orbital_angle, altitude_km, power_budget_w, eclipse, health, id
  const rows = sats.map(s => {
    const healthClass = {
      nominal:  'health-nominal',
      degraded: 'health-degraded',
      critical: 'health-critical',
    }[s.health?.toLowerCase()] || '';
    return `<tr>
      <td>${s.id}</td>
      <td>${s.orbital_angle?.toFixed(1) ?? '—'}°</td>
      <td>${s.altitude_km?.toFixed(0) ?? '—'} km</td>
      <td class="${healthClass}">${s.health ?? '—'}</td>
      <td>${s.power_budget_w ?? '—'} W</td>
      <td>${s.eclipse ? 'Yes' : 'No'}</td>
    </tr>`;
  }).join('');

  container.innerHTML = `
    <table>
      <thead>
        <tr>
          <th>ID</th><th>Orbital Angle</th><th>Altitude</th>
          <th>Health</th><th>Power Budget</th><th>Eclipse</th>
        </tr>
      </thead>
      <tbody>${rows}</tbody>
    </table>`;
}

// ─────────────────────────────────────────────────────────
// Schedules tab
// ─────────────────────────────────────────────────────────

// fetchSchedules calls GET /api/schedules.
// dashboard-api filters each schedule run: only assignments for the operator's
// gateway zone are returned. Alice never sees Bob's GW-SOUTH assignments.
async function fetchSchedules() {
  const container = document.getElementById('sched-container');
  container.innerHTML = '<p class="loading">Loading schedules…</p>';

  let res;
  try {
    res = await apiGet('/api/schedules');
  } catch (err) {
    container.innerHTML = '<p class="error-msg">Network error</p>';
    return;
  }
  if (!res) return;

  const data = await res.json();
  if (!res.ok) {
    container.innerHTML = `<p class="error-msg">${data.error}</p>`;
    return;
  }

  const schedules = data.schedules || [];
  if (schedules.length === 0) {
    container.innerHTML = '<p class="empty-msg">No schedule runs yet. Wait for the scheduler to run.</p>';
    return;
  }

  // Show most recent run first
  const cards = [...schedules].reverse().map(s => {
    // Assignment fields (snake_case from proto): satellite_id, gateway, status, eclipse, power_budget_w
    const items = (s.zone_assignments || []).map(a =>
      `<li>${a.satellite_id ?? '?'}  →  ${a.gateway}  [${a.status ?? ''}]  eclipse: ${a.eclipse ? 'yes' : 'no'}</li>`
    ).join('');
    return `<div class="schedule-card">
      <h3>Run #${s.run}</h3>
      <p>${s.timestamp}</p>
      <ul class="assign-list">${items || '<li style="color:#8b949e">No assignments for your zone</li>'}</ul>
    </div>`;
  }).join('');

  container.innerHTML = cards;
}

// ─────────────────────────────────────────────────────────
// Rate Limit Demo tab
// ─────────────────────────────────────────────────────────

// testRateLimit sends 12 POST /api/schedules/submit requests sequentially.
// Requests 1-10 should succeed (HTTP 200).
// Requests 11-12 should be rejected (HTTP 429) by dashboard-api's per-operator limiter.
// Each request is sent one after the other (not parallel) so the counter increments predictably.
async function testRateLimit() {
  const container = document.getElementById('rl-results');
  container.innerHTML = '';

  const PAYLOAD = {
    run: 99,
    ephemeris_version: 1,
    timestamp: new Date().toISOString(),
    scheduled_count: 0,
    skipped_count: 0,
    assignments: [],
  };

  for (let i = 1; i <= 12; i++) {
    let status, label, cls;
    try {
      const res = await apiPost('/api/schedules/submit', PAYLOAD);
      if (!res) return;               // 401 → logged out
      status = res.status;
    } catch (err) {
      status = 0;
    }

    if (status === 200) {
      label = '200 OK — accepted';
      cls   = 'rl-ok';
    } else if (status === 429) {
      label = '429 Too Many Requests — rate limited';
      cls   = 'rl-fail';
    } else {
      label = `${status} Unexpected`;
      cls   = 'rl-fail';
    }

    const row = document.createElement('div');
    row.className = 'rl-row';
    row.innerHTML = `
      <span class="rl-num">${i}</span>
      <span class="${cls}">${cls === 'rl-ok' ? '✓' : '✗'}</span>
      <span class="${cls}">${label}</span>`;
    container.appendChild(row);

    // Small delay so results appear one by one and don't hit Nginx IP rate limit
    await sleep(150);
  }
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}
