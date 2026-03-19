let allEntries = [];
let filteredEntries = [];
let sortState = { key: null, direction: 'asc' };
const fetchControllers = new Map();

document.addEventListener("DOMContentLoaded", async () => {

  const tableBody = document.querySelector("#entryTable tbody");
  const searchBox = document.getElementById("searchBox");
  const sortRes = document.getElementById("sortRes");
  const sortDate = document.getElementById("sortDate");

  const filterModal = document.getElementById("filterModal");
  const openFilterBtn = document.getElementById("openFilterBtn");
  const closeFilterBtn = document.getElementById("closeFilterBtn");
  const applyFilterBtn = document.getElementById("applyFilterBtn");
  const clearFilterBtn = document.getElementById("clearFilterBtn");
  const methodContainer = document.getElementById("methodFilterContainer");

  if (!tableBody) return;

  try {

    const response = await fetch("../data/json/entries_final.json");
    if (!response.ok) throw new Error("Failed to load entries_final.json");
    const data = await response.json();
    allEntries = Array.isArray(data) ? data : (data.entries || []);
    filteredEntries = [...allEntries];

    const methodOrder = ["X-ray", "NMR", "EM", "Neutron", "Other"]; 
    const foundMethods = [...new Set(allEntries.map(e => e.method).filter(Boolean))];
    
    const sortedMethods = foundMethods.sort((a, b) => {
        let idxA = methodOrder.indexOf(a);
        let idxB = methodOrder.indexOf(b);
        if (idxA === -1) idxA = 99;
        if (idxB === -1) idxB = 99;
        return idxA - idxB || a.localeCompare(b);
    });

    if (methodContainer) {
      methodContainer.innerHTML = sortedMethods.map(m => 
        `<label><input type="checkbox" class="method-filter" value="${m}"> ${m}</label>`
      ).join("");
    }

    renderTable(filteredEntries, tableBody);

    if (searchBox) {
      searchBox.addEventListener("input", () => {
        applyAllFilters();
        renderTable(filteredEntries, tableBody);
      });
    }

    if (sortRes) sortRes.addEventListener("click", () => handleSort('resolution', sortRes));
    if (sortDate) sortDate.addEventListener("click", () => handleSort('release_date', sortDate));

    if (openFilterBtn) openFilterBtn.addEventListener("click", () => filterModal.classList.remove("hidden"));
    if (closeFilterBtn) closeFilterBtn.addEventListener("click", () => filterModal.classList.add("hidden"));
    
    if (applyFilterBtn) {
      applyFilterBtn.addEventListener("click", () => {
        applyAllFilters();
        renderTable(filteredEntries, tableBody);
        filterModal.classList.add("hidden");
      });
    }

    if (clearFilterBtn) {
      clearFilterBtn.addEventListener("click", () => {
        document.querySelectorAll(".method-filter, .na-filter").forEach(cb => cb.checked = false);
        if (searchBox) searchBox.value = "";
        applyAllFilters();
        renderTable(filteredEntries, tableBody);
      });
    }

  } catch (error) {
    console.error("Initialization error:", error);
    tableBody.innerHTML = `<tr><td colspan="9" style="color:red; text-align:center;">Error: ${error.message}</td></tr>`;
  }
});

function applyAllFilters() {
  const searchBox = document.getElementById("searchBox");
  const query = searchBox ? searchBox.value.toLowerCase() : "";
  const selectedMethods = Array.from(document.querySelectorAll(".method-filter:checked")).map(cb => cb.value);
  const selectedNATypes = Array.from(document.querySelectorAll(".na-filter:checked")).map(cb => cb.value);

  filteredEntries = allEntries.filter(entry => {
    const naInfo = entry.na_info || "";
    const matchesSearch = entry.pdb_id.toLowerCase().includes(query);
    const matchesMethod = selectedMethods.length === 0 || selectedMethods.includes(entry.method);
    const matchesNA = selectedNATypes.length === 0 || selectedNATypes.includes(naInfo);

    return matchesSearch && matchesMethod && matchesNA;
  });

  if (sortState.key) {
    sortData(sortState.key, sortState.direction);
  }
}

function handleSort(key, element) {
  if (sortState.key === key) {
    sortState.direction = sortState.direction === 'asc' ? 'desc' : 'asc';
  } else {
    sortState.key = key;
    sortState.direction = 'asc';
  }

  document.querySelectorAll(".sortable").forEach(el => {
    el.textContent = el.textContent.replace(/[⇅▲▼]/g, '') + ' ⇅';
  });
  const arrow = sortState.direction === 'asc' ? '▲' : '▼';
  element.textContent = element.textContent.replace('⇅', arrow);

  sortData(key, sortState.direction);
  renderTable(filteredEntries, document.querySelector("#entryTable tbody"));
}

function sortData(key, direction) {
  filteredEntries.sort((a, b) => {
    let valA = a[key];
    let valB = b[key];
    if (valA == null) return 1;
    if (valB == null) return -1;
    if (valA < valB) return direction === 'asc' ? -1 : 1;
    if (valA > valB) return direction === 'asc' ? 1 : -1;
    return 0;
  });
}

function renderTable(entries, container) {
  if (!container) return;
  container.innerHTML = "";

  if (entries.length === 0) {
    container.innerHTML = '<tr><td colspan="9" style="text-align:center;">No entries found.</td></tr>';
    return;
  }

  entries.forEach((entry) => {
    const tr = document.createElement("tr");
    const options = (entry.assemblies || []).map(a => 
      `<option value="${a.assembly_id}">Assembly ${a.assembly_id}</option>`
    ).join("");

    tr.innerHTML = `
      <td class="pdb-id">${entry.pdb_id}</td>
      <td class="links-cell">
        <a href="${entry.rcsb_url}" target="_blank" class="btn-link-sm">PDB</a>
        <a href="${entry.nakb_url}" target="_blank" class="btn-link-sm">NAKB</a>
        <a href="${entry.view_structure_url}" target="_blank" class="btn-link-sm">View structure</a>
      </td>
      <td class="method-cell">${entry.method || ""}</td>
      <td class="res-cell">${entry.resolution ? entry.resolution.toFixed(2) + " Å" : ""}</td>
      <td class="assembly-cell">
        <select class="assembly-select">${options}</select>
      </td>
      <td class="na-info-cell">${entry.na_info || ""}</td>
      <td class="seq-cell">
        <div class="chain-sequence-container">${formatChains(entry.chains)}</div>
      </td>
      <td class="purified-cell"></td>
      <td class="release-date-cell">${entry.release_date || ""}</td>
    `;

    updatePurifiedLink(tr.querySelector(".purified-cell"), entry.purified_structure);

    const select = tr.querySelector(".assembly-select");
    if (select) {
      select.addEventListener("change", (e) => handleAssemblyChange(e, entry, tr));
    }

    container.appendChild(tr);
  });
}

function formatChains(chains) {
  if (!chains || !Array.isArray(chains)) return "";
  return chains.map(c => 
    `<div class="chain-item">
      <strong>Chain ${c.chain_id}</strong> (${c.na_type}):<br>
      <code class="seq-text">${c.sequence}</code>
    </div>`
  ).join("");
}

async function handleAssemblyChange(e, entry, tr) {
  const pdbId = entry.pdb_id;
  const newId = Number(e.target.value);

  if (fetchControllers.has(pdbId)) fetchControllers.get(pdbId).abort();
  const controller = new AbortController();
  fetchControllers.set(pdbId, controller);

  try {
    const response = await fetch(`../data/json/entries/${pdbId}.json`, { signal: controller.signal });
    if (!response.ok) throw new Error("Fetch failed");
    const detail = await response.json();
    const assembly = detail.assemblies.find(a => a.assembly_id === newId);
    
    if (assembly) {
      tr.querySelector(".na-info-cell").textContent = assembly.na_info || "";
      tr.querySelector(".seq-cell .chain-sequence-container").innerHTML = formatChains(assembly.chains);
      updatePurifiedLink(tr.querySelector(".purified-cell"), assembly.purified_structure);
    }
  } catch (error) {
    if (error.name !== 'AbortError') console.error("Assembly fetch error:", error);
  } finally {
    if (fetchControllers.get(pdbId) === controller) {
      fetchControllers.delete(pdbId);
    }
  }
}

function updatePurifiedLink(container, path) {
  if (!container) return;
  container.innerHTML = "";
  if (path) {
    const btn = document.createElement("a");
    btn.className = "btn-link";
    btn.href = `../${path}`;
    btn.download = "";
    btn.textContent = "Download";
    container.appendChild(btn);
  } else {
    container.textContent = "N/A";
  }
}