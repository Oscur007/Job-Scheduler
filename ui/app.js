const API = "";

const ICON_PATHS = {
  pending: '<circle cx="12" cy="12" r="9"/><path d="M12 7v5l3 2"/>',
  running: '<path d="M21 12a9 9 0 1 1-6.2-8.55"/><path d="M21 4v5h-5"/>',
  done:    '<circle cx="12" cy="12" r="9"/><path d="M8 12.5l3 3 5-6"/>',
  failed:  '<circle cx="12" cy="12" r="9"/><path d="M9 9l6 6M15 9l-6 6"/>'
};

function capitalize(s) {
  return s.charAt(0).toUpperCase() + s.slice(1);
}

// "send_email" -> "Send email"
function typeLabel(type) {
  const s = String(type).replace(/_/g, " ").trim();
  return s.charAt(0).toUpperCase() + s.slice(1);
}

function icon(status) {
  return `<svg class="icon ${status}" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">${ICON_PATHS[status] || ""}</svg>`;
}

const state = { page: 1, pageSize: 10 };

async function fetchJobs() {
  try {
    const [jobsRes, statsRes] = await Promise.all([
      fetch(`${API}/jobs?page=${state.page}&page_size=${state.pageSize}`),
      fetch(`${API}/stats`)
    ]);
    if (!jobsRes.ok || !statsRes.ok) throw new Error("bad response");
    const data = await jobsRes.json();
    const stats = await statsRes.json();

    // if the current page no longer exists (e.g. fewer jobs now), go to the last page
    const totalPages = Math.max(1, Math.ceil(data.total / state.pageSize));
    if (state.page > totalPages) {
      state.page = totalPages;
      return fetchJobs();
    }

    renderJobs(data.jobs || []);
    renderStats(stats);
    renderPager(data.total, totalPages);
  } catch (e) {
    document.getElementById("jobs-table").innerHTML = '<div class="empty">Could not reach the server.</div>';
  }
}

// counts come from the server, so they cover all jobs, not just the current page
function renderStats(stats) {
  const el = document.getElementById("stats");
  el.innerHTML = ["pending", "running", "done", "failed"].map(status =>
    `<div class="stat-chip">${icon(status)}<span><span class="stat-label">${capitalize(status)}</span> ${stats[status] || 0}</span></div>`
  ).join("");
}

function renderPager(total, totalPages) {
  const start = total === 0 ? 0 : (state.page - 1) * state.pageSize + 1;
  const end = Math.min(state.page * state.pageSize, total);
  document.getElementById("page-info").textContent =
    total === 0 ? "No jobs" : `Showing ${start}\u2013${end} of ${total}`;
  document.getElementById("page-label").textContent = `Page ${state.page} of ${totalPages}`;
  document.getElementById("prev-page").disabled = state.page <= 1;
  document.getElementById("next-page").disabled = state.page >= totalPages;
}

function renderJobs(jobs) {
  const el = document.getElementById("jobs-table");
  if (!jobs.length) {
    el.innerHTML = '<div class="empty">No jobs yet. Submit one to get started.</div>';
    return;
  }
  const rows = jobs.map(j => `
    <tr>
      <td class="id-cell">${j.id.slice(0, 8)}</td>
      <td class="type-cell">${typeLabel(j.type)}</td>
      <td><span class="badge ${j.status}">${icon(j.status)}${j.status}</span></td>
      <td>${j.priority}</td>
      <td class="${j.max_retries > 0 && j.retries >= j.max_retries ? 'retries-exhausted' : ''}">${j.retries}/${j.max_retries}</td>
      <td>${new Date(j.created_at).toLocaleTimeString()}</td>
    </tr>
  `).join("");
  el.innerHTML = `
    <table>
      <thead>
        <tr><th>ID</th><th>Type</th><th>Status</th><th>Priority</th><th>Retries</th><th>Created</th></tr>
      </thead>
      <tbody>${rows}</tbody>
    </table>
  `;
}

async function submitJob() {
  const body = {
    type: document.getElementById("type").value,
    payload: document.getElementById("payload").value,
    priority: parseInt(document.getElementById("priority").value) || 0,
    delay_seconds: parseInt(document.getElementById("delay").value) || 0,
    max_retries: parseInt(document.getElementById("max_retries").value) || 3
  };
  try {
    const res = await fetch(API + "/jobs", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body)
    });
    if (!res.ok) throw new Error("Request failed");
    showToast("Job submitted");
    fetchJobs();
  } catch (e) {
    showToast("Failed to submit job");
  }
}

function showToast(msg) {
  const t = document.getElementById("toast");
  t.textContent = msg;
  t.classList.add("show");
  setTimeout(() => t.classList.remove("show"), 2000);
}

document.getElementById("page-size").addEventListener("change", e => {
  state.pageSize = parseInt(e.target.value, 10);
  state.page = 1;
  fetchJobs();
});
document.getElementById("prev-page").addEventListener("click", () => {
  if (state.page > 1) { state.page--; fetchJobs(); }
});
document.getElementById("next-page").addEventListener("click", () => {
  state.page++;
  fetchJobs();
});

fetchJobs();
setInterval(fetchJobs, 3000);