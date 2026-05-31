// Database Connection Configuration Strings
const supabaseUrl = 'https://jscbtuvnjoinzmfzyqry.supabase.co';
const supabaseKey = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImpzY2J0dXZuam9pbnptZnp5cXJ5Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3Nzk5NjI2OTksImV4cCI6MjA5NTUzODY5OX0.5-UFYNEoh7OXU9V8KpH35nR4jBuWULSPaj4JDdyBCu4';

const supabaseClient = window.supabase.createClient(supabaseUrl, supabaseKey);

// Shared Global Memory Base State Bus
let dbProducts = [];
let dbMaterials = [];
let dbHardware = [];
let dbConfigs = {};
let liveJobsPipeline = [];

// Staging Memory Matrix Structures for Active Customer Configuration
let activeBuildModulesListArray = [];
let activeJobObstructions = [];
let activeObstructionsList = [];
let inputModeType = "stepper"; 
let currentOutstandingFilterMode = "all"; 

// Outstanding Jobs Split Layout Mode Parameters
let currentOutstandingViewMode = "list"; 
let outstandingCalYear = 2026;
let outstandingCalMonth = 5; 

// High-UX Intake Modal Month State Tracking Parameters
let currentModalYear = 2026;
let currentModalMonth = 5; 
let selectedModalIntakeDateStr = "";
let selectedModalIntakeTimeStr = "";
let calculatedOrderTotalGrossValue = 0;

// Initialize Application Engine on Layout Load
document.addEventListener('DOMContentLoaded', async () => {
    console.log("PGS Core Engine Initialised...");
    const baseNowDate = new Date();
    currentModalYear = baseNowDate.getFullYear();
    currentModalMonth = baseNowDate.getMonth();
    outstandingCalYear = baseNowDate.getFullYear();
    outstandingCalMonth = baseNowDate.getMonth();

    await syncStateWithDatabaseCluster();
    configureSystemIntervalAlarms();
    configureRealtimeSubscriptions();
});

// ── Debounce helper — prevents rapid-fire re-renders on bulk changes ──
function debounce(fn, ms) {
    let timer;
    return (...args) => { clearTimeout(timer); timer = setTimeout(() => fn(...args), ms); };
}

const debouncedSync = debounce(syncStateWithDatabaseCluster, 400);

// ── Realtime subscription engine ──
function configureRealtimeSubscriptions() {
    const channel = supabaseClient
        .channel('pgs-realtime-bus')

        // job_ledger — any row change
        .on('postgres_changes', { event: '*', schema: 'public', table: 'job_ledger' }, () => {
            debouncedSync();
        })

        // configuration_ledger — covers expenses, transactions, and config changes
        .on('postgres_changes', { event: '*', schema: 'public', table: 'configuration_ledger' }, () => {
            debouncedSync();
        })

        .subscribe(status => {
            setRealtimeStatusIndicator(status);
        });
}

// ── Update the footer status dot ──
function setRealtimeStatusIndicator(status) {
    const dot   = document.getElementById('realtime-status-dot');
    const label = document.getElementById('realtime-status-label');
    if (!dot || !label) return;

    if (status === 'SUBSCRIBED') {
        dot.className   = 'w-2 h-2 rounded-full bg-emerald-400 shadow-[0_0_6px_rgba(52,211,153,0.8)]';
        label.textContent = 'Live';
        label.className   = 'text-[9px] uppercase font-bold tracking-tighter text-emerald-400';
    } else if (status === 'CHANNEL_ERROR' || status === 'TIMED_OUT' || status === 'CLOSED') {
        dot.className   = 'w-2 h-2 rounded-full bg-rose-400';
        label.textContent = 'Offline';
        label.className   = 'text-[9px] uppercase font-bold tracking-tighter text-rose-400';
    } else {
        dot.className   = 'w-2 h-2 rounded-full bg-amber-400 animate-pulse';
        label.textContent = 'Connecting';
        label.className   = 'text-[9px] uppercase font-bold tracking-tighter text-amber-400';
    }
}

// ── Shared config save helper ──────────────────────────────────────────────────
// Writes a single key/value to configuration_ledger.
// Works with or without a UNIQUE constraint on key.
// THROWS on failure so callers can catch and show real error feedback.
async function upsertConfig(key, value) {
    const val = String(value ?? '');
    dbConfigs[key] = val;

    // Use limit(1) — maybeSingle() errors when duplicate rows exist
    // Select 'key' not 'id' — configuration_ledger primary key IS the key column
    const { data: rows } = await supabaseClient
        .from('configuration_ledger')
        .select('key')
        .eq('key', key)
        .limit(1);

    const exists = rows && rows.length > 0;
    const { error } = exists
        ? await supabaseClient.from('configuration_ledger').update({ value: val }).eq('key', key)
        : await supabaseClient.from('configuration_ledger').insert({ key, value: val });

    if (error) throw new Error(`[${key}] ${error.message}`);
}

// Debounced auto-save for scheduling settings — fires 900 ms after the last change
const _debouncedSaveScheduling = debounce(async () => {
    try {
        await Promise.all([
            upsertConfig('available_weekdays',  dbConfigs['available_weekdays']  ?? '1,2,3,4,5'),
            upsertConfig('available_time_slots', dbConfigs['available_time_slots'] ?? '08:00,11:00,14:00'),
        ]);
        showToastNotification('Scheduling saved ✓');
    } catch (e) {
        console.error('Scheduling auto-save failed:', e.message);
        showToastNotification('Scheduling save failed ❌');
    }
}, 900);

// Global Core Sync Logic Pulling Live Database Tables
async function syncStateWithDatabaseCluster() {
    try {
        const { data: prods } = await supabaseClient.from('product_templates').select('*').order('name');
        const { data: mats } = await supabaseClient.from('materials_inventory').select('*').order('name');
        const { data: hard } = await supabaseClient.from('hardware_inventory').select('*').order('name');
        const { data: configs } = await supabaseClient.from('configuration_ledger').select('*');
        const { data: jobs } = await supabaseClient.from('job_ledger').select('*').order('created_at', { ascending: false });

        dbProducts = prods || [];
        dbMaterials = mats || [];
        dbHardware = hard || [];
        liveJobsPipeline = jobs || [];

        if (configs) configs.forEach(c => dbConfigs[c.key] = c.value);

        // Load expense log from config blob (keyed separately)
        await loadExpensesFromDB();
        // Load transaction ledger from config blob
        await loadTransactionsFromDB();
        // Load product recipes
        await loadRecipesFromDB();

        applyBrandConfigAssets();
        renderDropdownSelectionProfiles();
        renderDynamicConfiguratorParametersControls();
        executeCoreAnalyticalMathEngineRuns();
        renderOutstandingJobsMatrixRegistryDeck();
        buildWeeklyBatchFilterDropdownSlices();
        calculateGlobalTurnoverLedgerComplianceSplits();
        renderSystemSettingsCRUDForms();
        buildProductionScheduleCalendarGrid();
        renderRecipeEditor();
    } catch (ex) {
        console.error("Supabase transport connection trace fault:", ex.message);
    }
}

// Inject Custom Logo Assets into Header Slot
function applyBrandConfigAssets() {
    const assetUrl = dbConfigs['logo_asset_url'];
    const container = document.getElementById('logo-container');
    if (container && assetUrl) container.innerHTML = `<img src="${assetUrl}" class="w-full h-full object-cover rounded-xl" alt="PGS">`;
}

// Layout Tab Router Controller Navigation
function switchTab(targetTabId) {
    document.querySelectorAll('.tab-content').forEach(el => el.classList.add('hidden'));
    document.getElementById(targetTabId).classList.remove('hidden');
    document.querySelectorAll('.tab-btn').forEach(btn => {
        btn.classList.remove('bg-brand-500', 'text-slate-950');
        btn.classList.add('text-slate-400', 'hover:text-white');
    });
    const matchedBtn = Array.from(document.querySelectorAll('.tab-btn')).find(b => b.getAttribute('onclick').includes(targetTabId));
    if (matchedBtn) {
        matchedBtn.classList.remove('text-slate-400', 'hover:text-white');
        matchedBtn.classList.add('bg-brand-500', 'text-slate-950');
    }

    // Redraw canvas chart now that the tab is visible and has real dimensions
    if (targetTabId === 'finance-tab') {
        requestAnimationFrame(() => {
            const canvas = document.getElementById('fin-monthly-chart');
            if (canvas && canvas._chartData) {
                renderMonthlyRevenueChart(canvas._chartData.monthlyTotals, canvas._chartData.year);
            }
        });
    }
}

// Populate Planner Package Selection Options Dropdown
function renderDropdownSelectionProfiles() {
    const dropdown     = document.getElementById('showroom-template-dropdown');
    const editDropdown = document.getElementById('edit-module-dropdown');
    const options = dbProducts.map(p => `<option value="${p.id}">${p.name} — £${parseFloat(p.base_retail_price).toFixed(2)}</option>`).join('');
    if (dropdown)     dropdown.innerHTML     = options;
    if (editDropdown) editDropdown.innerHTML = options;
}

// Append Selected Module Template to the Multi-Item Customer Assembly List
function addTemplateToActiveCart() {
    const dropdown = document.getElementById('showroom-template-dropdown');
    if (!dropdown.value) return;
    const item = dbProducts.find(p => p.id === dropdown.value);
    if (!item) return;

    activeBuildModulesListArray.push({
        ...item,
        cart_item_uuid: crypto.randomUUID(),
        workingPrice: parseFloat(item.base_retail_price),
        width_mm: parseInt(item.width_mm),
        depth_mm: parseInt(item.depth_mm),
        height_mm: parseInt(item.height_mm),
        shelves_count: (() => { const r = dbRecipes[item.id]; return parseInt(r != null && r.default_shelves != null ? r.default_shelves : (item.shelves_count || 4)); })(),
        shelf_height_mm: (() => { const r = dbRecipes[item.id]; return r?.shelf_height_mm ?? defaultBuildSpec(item).shelf_height_mm; })()
    });

    renderActiveStagedModulesListLayout();
    renderDynamicConfiguratorParametersControls();
    executeCoreAnalyticalMathEngineRuns();
    renderObstructionPositionDropdown();
}

// Reorder Staged Modules Positionally Left-to-Right in the Configuration Array Matrix
function moveModuleInCart(index, direction) {
    const targetIndex = index + direction;
    if (targetIndex < 0 || targetIndex >= activeBuildModulesListArray.length) return;

    const tempHolder = activeBuildModulesListArray[index];
    activeBuildModulesListArray[index] = activeBuildModulesListArray[targetIndex];
    activeBuildModulesListArray[targetIndex] = tempHolder;

    renderActiveStagedModulesListLayout();
    renderDynamicConfiguratorParametersControls();
    executeCoreAnalyticalMathEngineRuns();
    renderObstructionPositionDropdown();
}

// Delete Module Item from Active Assembly List Array
function removeStagedModuleFromCart(uuid) {
    activeBuildModulesListArray = activeBuildModulesListArray.filter(i => i.cart_item_uuid !== uuid);
    // Clamp any obstruction positions that are now out of range
    const maxIdx = activeBuildModulesListArray.length - 1;
    activeObstructionsList.forEach(o => {
        if (o.insertAfterBayIndex > maxIdx) o.insertAfterBayIndex = maxIdx;
    });
    renderActiveStagedModulesListLayout();
    renderDynamicConfiguratorParametersControls();
    executeCoreAnalyticalMathEngineRuns();
    renderObstructionPositionDropdown();
    renderActiveObstructionsList();
    renderWireframeGraphicCanvas();
}

// ── Obstruction Management ──

const OBSTRUCTION_DEFAULTS = { column: 100, door: 900, window: 600, gap: 200 };
const OBSTRUCTION_LABELS   = { column: 'Column', door: 'Door', window: 'Window', gap: 'Gap' };

function updateObstructionDefaultWidth() {
    const type  = document.getElementById('obstruction-type-select')?.value;
    const input = document.getElementById('obstruction-width-input');
    if (input && type) input.value = OBSTRUCTION_DEFAULTS[type] ?? 100;
}

function renderObstructionPositionDropdown() {
    const sel = document.getElementById('obstruction-position-select');
    if (!sel) return;
    const current = sel.value;
    const opts = [`<option value="-1">At the start</option>`];
    activeBuildModulesListArray.forEach((_, i) => {
        opts.push(`<option value="${i}">After Bay ${i + 1}</option>`);
    });
    sel.innerHTML = opts.join('');
    // Restore previous selection if still valid
    if ([...sel.options].some(o => o.value === current)) sel.value = current;
}

function addObstructionToCanvas() {
    const type  = document.getElementById('obstruction-type-select')?.value || 'column';
    const width = parseInt(document.getElementById('obstruction-width-input')?.value) || OBSTRUCTION_DEFAULTS[type];
    const pos   = parseInt(document.getElementById('obstruction-position-select')?.value ?? '-1');

    activeObstructionsList.push({
        uuid: crypto.randomUUID(),
        type,
        width_mm: Math.max(10, Math.min(3000, width)),
        insertAfterBayIndex: pos,
        label: OBSTRUCTION_LABELS[type] || 'Obstruction'
    });

    renderActiveObstructionsList();
    renderWireframeGraphicCanvas();
}

function removeObstructionFromCanvas(uuid) {
    activeObstructionsList = activeObstructionsList.filter(o => o.uuid !== uuid);
    renderActiveObstructionsList();
    renderWireframeGraphicCanvas();
}

function renderActiveObstructionsList() {
    const container = document.getElementById('active-obstructions-list');
    if (!container) return;

    if (activeObstructionsList.length === 0) {
        container.innerHTML = '';
        return;
    }

    container.innerHTML = activeObstructionsList.map(o => {
        const posLabel = o.insertAfterBayIndex === -1
            ? 'Start'
            : `After Bay ${o.insertAfterBayIndex + 1}`;
        return `
        <div class="flex items-center justify-between gap-2 p-2 bg-slate-900 border border-slate-800 rounded-xl text-xs">
            <div class="truncate flex-1">
                <span class="text-rose-300 font-bold block truncate">${o.label}</span>
                <span class="text-[11px] text-slate-400 block">${o.width_mm}mm · ${posLabel}</span>
            </div>
            <button onclick="removeObstructionFromCanvas('${o.uuid}')" class="bg-rose-500/10 text-rose-400 px-2 py-0.5 rounded font-bold">X</button>
        </div>`;
    }).join('');
}

// Returns an ordered sequence of { _type:'module'|'obstruction', ...fields } for the canvas
function buildMergedCanvasSequence() {
    const seq = [];

    // Obstructions before all bays
    activeObstructionsList
        .filter(o => o.insertAfterBayIndex === -1)
        .forEach(o => seq.push({ ...o, _type: 'obstruction' }));

    activeBuildModulesListArray.forEach((module, bayIdx) => {
        seq.push({ ...module, _type: 'module' });
        activeObstructionsList
            .filter(o => o.insertAfterBayIndex === bayIdx)
            .forEach(o => seq.push({ ...o, _type: 'obstruction' }));
    });

    return seq;
}

// Display Active Staged Modules List Components Layout
function renderActiveStagedModulesListLayout() {
    const container = document.getElementById('active-staged-modules-list');
    if (!container) return;

    if (activeBuildModulesListArray.length === 0) {
        container.innerHTML = `<p class="text-slate-500 text-xs italic p-2 text-center">No modules added yet. Select a type above and hit +.</p>`;
        return;
    }

    container.innerHTML = activeBuildModulesListArray.map((item, index) => `
        <div class="flex items-center justify-between gap-2 p-2 bg-slate-900 border border-slate-800 rounded-xl text-xs">
            <div class="truncate flex-1">
                <span class="text-white font-bold block truncate">${item.name}</span>
                <span class="text-[11px] text-slate-400 block">${item.width_mm}mm W × ${item.depth_mm}mm D</span>
            </div>
            <div class="flex items-center gap-1.5">
                <button onclick="moveModuleInCart(${index}, -1)" class="bg-slate-800 text-slate-300 w-5 h-5 flex items-center justify-center rounded text-[10px] hover:text-brand-400 font-bold disabled:opacity-20 transition-all" ${index === 0 ? 'disabled' : ''}>←</button>
                <button onclick="moveModuleInCart(${index}, 1)" class="bg-slate-800 text-slate-300 w-5 h-5 flex items-center justify-center rounded text-[10px] hover:text-brand-400 font-bold disabled:opacity-20 transition-all" ${index === activeBuildModulesListArray.length - 1 ? 'disabled' : ''}>→</button>
                <span class="text-brand-400 font-bold ml-1">£${item.workingPrice.toFixed(0)}</span>
                <button onclick="removeStagedModuleFromCart('${item.cart_item_uuid}')" class="bg-rose-500/10 text-rose-400 px-2 py-0.5 rounded font-bold ml-1">X</button>
            </div>
        </div>
    `).join('');
}

// Dynamically Render Live Shelf Quantity Controls Board inside Sidebar Container
function renderDynamicConfiguratorParametersControls() {
    const container = document.getElementById('parameter-inputs-container');
    if (!container) return;

    if (activeBuildModulesListArray.length === 0) {
        container.innerHTML = `<p class="text-xs text-slate-400 italic leading-normal">Multi-module staging configuration active. Choose frame variants to compound system dimensions parameters.</p>`;
        return;
    }

    container.innerHTML = `
        <div class="space-y-3">
            <span class="block text-xs font-bold text-slate-400 uppercase tracking-wider">Configure Shelf Tiers Per Bay</span>
            ${activeBuildModulesListArray.map((item, idx) => `
                <div class="bg-slate-900 p-2.5 rounded-xl border border-slate-800 text-xs shadow-inner space-y-2">
                    <div class="flex items-center justify-between">
                        <span class="text-slate-300 truncate max-w-[140px] font-bold">Bay ${idx + 1}: ${item.name}</span>
                        <div class="flex items-center gap-2.5">
                            <button onclick="adjustStagedBayShelfCount('${item.cart_item_uuid}', -1)" class="w-6 h-6 bg-slate-800 text-white font-black rounded flex items-center justify-center hover:bg-brand-500 hover:text-slate-950 transition-all">-</button>
                            <span class="text-brand-400 font-black w-4 text-center text-sm">${item.shelves_count}</span>
                            <button onclick="adjustStagedBayShelfCount('${item.cart_item_uuid}', 1)" class="w-6 h-6 bg-slate-800 text-white font-black rounded flex items-center justify-center hover:bg-brand-500 hover:text-slate-950 transition-all">+</button>
                        </div>
                    </div>
                    ${item.shelf_height_mm != null ? `
                    <div class="flex items-center justify-between gap-2 pt-1 border-t border-slate-800">
                        <span class="text-slate-500 text-[10px] uppercase font-bold">Work surface height</span>
                        <div class="flex items-center gap-1.5">
                            <input type="number" value="${item.shelf_height_mm}" min="100" max="${item.height_mm - 100}" step="10"
                                oninput="setModuleShelfHeight('${item.cart_item_uuid}', this.value)"
                                class="w-20 bg-slate-800 border border-slate-700 rounded-lg p-1.5 text-white text-xs text-right outline-none focus:border-brand-500">
                            <span class="text-slate-500 text-[10px]">mm</span>
                        </div>
                    </div>` : ''}
                </div>
            `).join('')}
        </div>
    `;
}

// Modify Shelf Parameter Values directly inside Array Configuration Registers
function adjustStagedBayShelfCount(uuid, delta) {
    const targetItem = activeBuildModulesListArray.find(i => i.cart_item_uuid === uuid);
    if (!targetItem) return;

    targetItem.shelves_count = Math.max(1, Math.min(10, targetItem.shelves_count + delta));

    executeCoreAnalyticalMathEngineRuns();
    renderActiveStagedModulesListLayout();
    renderDynamicConfiguratorParametersControls();
}

function setModuleShelfHeight(uuid, value) {
    const item = activeBuildModulesListArray.find(i => i.cart_item_uuid === uuid);
    if (!item) return;
    const parsed = parseInt(value);
    if (isNaN(parsed)) return;
    item.shelf_height_mm = Math.max(100, Math.min(item.height_mm - 100, parsed));
    renderWireframeGraphicCanvas();
    executeCoreAnalyticalMathEngineRuns();
}

// Calculate Dynamic Retail Price Points for Active Assembly Items list factoring shelf fluctuations
// Calculate Dynamic Retail Price Points for Active Assembly Items list factoring shelf fluctuations
function executeCoreAnalyticalMathEngineRuns() {
    calculatedOrderTotalGrossValue = 0;

    activeBuildModulesListArray.forEach(item => {
        const baseTemplate = dbProducts.find(p => p.id === item.id);
        const recipeSpec = dbRecipes[item.id];
        const defaultShelves = recipeSpec?.default_shelves ?? (baseTemplate ? parseInt(baseTemplate.shelves_count || 4) : 4);
        const extraShelfCostFactor = parseFloat(dbConfigs['shelf_tier_modifier_price'] || 15.00);
        
        item.workingPrice = parseFloat(item.base_retail_price) + ((item.shelves_count - defaultShelves) * extraShelfCostFactor);
        calculatedOrderTotalGrossValue += item.workingPrice;
    });

    const priceBox = document.getElementById('showroom-live-price-box');
    if (priceBox) {
        priceBox.innerHTML = `<span class="text-slate-400 text-xs font-bold uppercase">Total Combined Price:</span><span class="text-2xl font-black text-emerald-400">£${calculatedOrderTotalGrossValue.toFixed(2)}</span>`;
    }

    renderWireframeGraphicCanvas();
    
    // LIVE FIX INJECTION: Forces Bill of Materials update on every cycle run
    renderLiveBOM();
}

// Dedicated Multi-Module Staging Material Recipe Renderer
function renderLiveBOM() {
    const bomContainer = document.getElementById('planner-bom-container');
    if (!bomContainer) return;

    if (activeBuildModulesListArray.length === 0) {
        bomContainer.innerHTML = `
            <div class="text-center py-6 text-slate-500 text-xs italic">
                Add modules to the staging area to generate a live material recipe breakdown.
            </div>
        `;
        return;
    }

    let totalTimberMeters = 0;
    let totalScrewsCount = 0;
    let totalShelvesCount = 0;
    let breakdownHTML = '';

    // Walk through active planner array to derive recipe calculations reactively
    activeBuildModulesListArray.forEach((item, index) => {
        const W = parseInt(item.width_mm) || 0;
        const D = parseInt(item.depth_mm) || 0;
        const H = parseInt(item.height_mm) || 0;
        const S = parseInt(item.shelves_count) || 4;

        // Formula definitions matching your framework specs
        const uprightsTimber = (4 * H) / 1000; // 4 structural posts
        const horizontalPerimeterRails = ((2 * W) + (2 * D)) / 1000;
        const nogginsPerShelf = Math.max(1, Math.round(W / 610));
        const nogginsTimber = nogginsPerShelf * (D / 1000);
        
        const totalTimberPerShelfTier = horizontalPerimeterRails + nogginsTimber;
        const totalModuleTimberMeters = uprightsTimber + (S * totalTimberPerShelfTier);
        
        // Hardware: 8 structural heavy fixings per shelf level + 16 base frame anchoring fixings
        const totalModuleScrews = (S * 8) + 16;

        totalTimberMeters += totalModuleTimberMeters;
        totalScrewsCount += totalModuleScrews;
        totalShelvesCount += S;

        breakdownHTML += `
            <div class="p-2.5 bg-slate-900/70 border border-slate-800 rounded-xl flex justify-between items-center text-xs">
                <div>
                    <span class="font-bold text-slate-200 block">Bay ${index + 1}: ${item.name}</span>
                    <span class="text-[10px] text-slate-400 font-mono">${W}mm W × ${D}mm D × ${H}mm H</span>
                </div>
                <div class="text-right font-mono">
                    <span class="text-brand-400 font-bold block">${S} Tiers</span>
                    <span class="text-[10px] text-slate-500">${totalModuleTimberMeters.toFixed(2)}m | ${totalModuleScrews} pcs</span>
                </div>
            </div>
        `;
    });

    bomContainer.innerHTML = `
        <div class="space-y-4">
            <div class="grid grid-cols-2 gap-3">
                <div class="bg-slate-900 border border-slate-800/80 p-3 rounded-xl shadow-inner">
                    <span class="text-[10px] uppercase font-bold text-slate-500 tracking-wider block">Linear Timber</span>
                    <span class="text-base font-black text-brand-400 font-mono">${totalTimberMeters.toFixed(2)}m</span>
                </div>
                <div class="bg-slate-900 border border-slate-800/80 p-3 rounded-xl shadow-inner">
                    <span class="text-[10px] uppercase font-bold text-slate-500 tracking-wider block">Heavy Duty Screws</span>
                    <span class="text-base font-black text-emerald-400 font-mono">${totalScrewsCount} pcs</span>
                </div>
            </div>

            <div class="flex justify-between items-center text-[10px] font-bold text-slate-400 uppercase tracking-wider pt-0.5">
                <span>Material Recipe Breakdown</span>
                <span class="bg-slate-800 text-slate-300 px-2 py-0.5 rounded font-mono">${totalShelvesCount} Total Tiers</span>
            </div>

            <div class="space-y-2 max-h-44 overflow-y-auto custom-scrollbar pr-1">
                ${breakdownHTML}
            </div>
        </div>
    `;
}

// Render realistic SVG shelf unit canvas
function renderWireframeGraphicCanvas() {
    const canvas = document.getElementById('canvas-blueprint-viewport');
    if (!canvas) return;
    canvas.innerHTML = "";

    if (activeBuildModulesListArray.length === 0 && activeObstructionsList.length === 0) {
        canvas.innerHTML = `<div class="text-slate-600 text-xs select-none">Staging structural framework schematics...</div>`;
        return;
    }

    const UPRIGHT_W = 38;   // CLS width mm
    const SHELF_T   = 18;   // OSB thickness mm
    const FOOT_H    = 60;   // floor clearance mm
    const LABEL_H   = 30;   // top label space px
    const DIM_H     = 32;   // bottom dimension space px

    const seq     = buildMergedCanvasSequence();
    const modules = seq.filter(i => i._type === 'module');
    const maxH_mm = modules.length > 0 ? Math.max(...modules.map(m => m.height_mm)) : 600;
    const floorMm = maxH_mm + FOOT_H;

    // Walk sequence to find the rightmost upright edge for total canvas width.
    // When an obstruction follows a module its right upright (UPRIGHT_W) sits in the gap,
    // so we advance the cursor by UPRIGHT_W before placing the obstruction.
    let _rawCursor = 0;
    const _rawPos = seq.map((item, idx) => {
        if (item._type === 'obstruction' && idx > 0 && seq[idx - 1]._type === 'module') _rawCursor += UPRIGHT_W;
        const startX = _rawCursor;
        _rawCursor += item.width_mm;
        return { item, startX };
    });
    let totalW_mm = 0;
    _rawPos.forEach(({ item, startX }) => {
        totalW_mm = Math.max(totalW_mm,
            item._type === 'module' ? startX + item.width_mm + UPRIGHT_W : startX + item.width_mm);
    });
    if (totalW_mm === 0) totalW_mm = 100;

    const vw     = canvas.clientWidth  || 600;
    const vh     = canvas.clientHeight || 340;
    const availW = vw  - 32;
    const availH = vh  - LABEL_H - DIM_H - 16;
    const scale  = Math.min(availW / totalW_mm, availH / floorMm);

    const svgW = totalW_mm * scale + 32;
    const svgH = floorMm   * scale + LABEL_H + DIM_H + 12;

    const C = {
        timber:      '#c8a96e',
        timberDark:  '#a07840',
        timberGrain: '#b89458',
        osb:         '#d4a853',
        osbEdge:     '#b08030',
        osbSpot:     'rgba(0,0,0,0.07)',
        wall:        '#1e293b',
        anchor:      '#94a3b8',
        anchorHead:  '#e2e8f0',
        shadow:      'rgba(0,0,0,0.3)',
        label:       '#FF8700',
        labelBg:     '#0f172a',
        dim:         '#475569',
        spacing:     '#38bdf8',
        obsBlock:    '#1e293b',
        obsHatch:    '#334155',
        obsLabel:    '#f87171',
        obsDim:      '#94a3b8',
    };

    const ns  = 'http://www.w3.org/2000/svg';
    const svg = document.createElementNS(ns, 'svg');
    svg.setAttribute('width',  svgW);
    svg.setAttribute('height', svgH);
    svg.style.display  = 'block';
    svg.style.margin   = 'auto';
    svg.style.overflow = 'visible';

    const defs = document.createElementNS(ns, 'defs');

    // Timber gradient
    const tg = document.createElementNS(ns, 'linearGradient');
    tg.setAttribute('id','pgs-timber2'); tg.setAttribute('x1','0'); tg.setAttribute('x2','1'); tg.setAttribute('y1','0'); tg.setAttribute('y2','0');
    [['0%','#dbbe80'],['40%',C.timber],['70%',C.timber],['100%',C.timberDark]].forEach(([o,c])=>{
        const s=document.createElementNS(ns,'stop'); s.setAttribute('offset',o); s.setAttribute('stop-color',c); tg.appendChild(s);
    });
    defs.appendChild(tg);

    // OSB gradient
    const og = document.createElementNS(ns, 'linearGradient');
    og.setAttribute('id','pgs-osb2'); og.setAttribute('x1','0'); og.setAttribute('x2','0'); og.setAttribute('y1','0'); og.setAttribute('y2','1');
    [['0%','#e8c070'],['40%',C.osb],['100%',C.osbEdge]].forEach(([o,c])=>{
        const s=document.createElementNS(ns,'stop'); s.setAttribute('offset',o); s.setAttribute('stop-color',c); og.appendChild(s);
    });
    defs.appendChild(og);

    // Cross-hatch pattern for obstructions
    const hp = document.createElementNS(ns, 'pattern');
    hp.setAttribute('id','obs-hatch'); hp.setAttribute('width','8'); hp.setAttribute('height','8');
    hp.setAttribute('patternUnits','userSpaceOnUse'); hp.setAttribute('patternTransform','rotate(45)');
    const hl = document.createElementNS(ns, 'line');
    hl.setAttribute('x1','0'); hl.setAttribute('y1','0'); hl.setAttribute('x2','0'); hl.setAttribute('y2','8');
    hl.setAttribute('stroke', C.obsHatch); hl.setAttribute('stroke-width','2');
    hp.appendChild(hl);
    defs.appendChild(hp);

    svg.appendChild(defs);

    function el(tag, attrs, parent) {
        const e = document.createElementNS(ns, tag);
        Object.entries(attrs).forEach(([k,v]) => e.setAttribute(k, v));
        if (parent) parent.appendChild(e);
        return e;
    }
    function txt(content, attrs, parent) {
        const e = document.createElementNS(ns, 'text');
        Object.entries(attrs).forEach(([k,v]) => e.setAttribute(k,v));
        e.textContent = content;
        if (parent) parent.appendChild(e);
        return e;
    }
    function drawUpright(x, topY, height, showAnchors) {
        const uW = UPRIGHT_W * scale;
        const h  = height * scale;
        el('rect', { x, y: topY, width: uW, height: h, fill: 'url(#pgs-timber2)' }, svg);
        for (let g = 0; g < 3; g++) {
            el('line', {
                x1: x + uW*(0.22+g*0.22), y1: topY+3,
                x2: x + uW*(0.26+g*0.22), y2: topY+h-3,
                stroke: C.timberGrain, 'stroke-width': '0.7', opacity: '0.45'
            }, svg);
        }
        if (showAnchors) {
            [0.15, 0.5, 0.85].forEach(frac => {
                const ay = topY + h * frac;
                el('rect',   { x: x+uW*0.15, y: ay-2.5*scale, width: uW*0.7, height: 5*scale, fill: C.anchor, rx: 1 }, svg);
                el('circle', { cx: x+uW*0.5, cy: ay, r: 2.2*scale, fill: C.anchorHead }, svg);
            });
        }
    }
    function drawShelf(x, y, width) {
        const sT = SHELF_T * scale;
        el('rect', { x, y, width, height: sT, fill: 'url(#pgs-osb2)' }, svg);
        el('rect', { x, y: y+sT-2, width, height: 2, fill: C.osbEdge, opacity: '0.8' }, svg);
        for (let sp = 0; sp < 5; sp++) {
            el('ellipse', { cx: x+width*(0.08+sp*0.19), cy: y+sT*0.45, rx: width*0.035, ry: sT*0.28, fill: C.osbSpot }, svg);
        }
    }

    // ── Back wall ──
    const wallX = 16;
    const wallY = LABEL_H;
    const wallW = totalW_mm * scale;
    const wallH = floorMm   * scale;
    el('rect', { x: wallX, y: wallY, width: wallW, height: wallH, fill: C.wall, rx: 3 }, svg);

    // ── Floor shadow & line ──
    el('ellipse', { cx: wallX+wallW/2, cy: wallY+wallH+5, rx: wallW*0.46, ry: 6, fill: C.shadow }, svg);
    el('line', { x1: wallX, y1: wallY+wallH, x2: wallX+wallW, y2: wallY+wallH, stroke: '#334155', 'stroke-width': '1.5' }, svg);

    // ── Build item geometries from merged sequence ──
    // Reuse the pre-computed raw positions (already gap-corrected) to derive pixel coords.
    const itemGeos = _rawPos.map(({ item, startX: startX_mm }) => ({
        item, startX_mm,
        px: wallX + startX_mm * scale,
        pw: item.width_mm * scale,
    }));

    // Collect upright positions (left + right edge of every module)
    const uprightSet = new Set();
    itemGeos.forEach(({ item, startX_mm }) => {
        if (item._type === 'module') {
            uprightSet.add(startX_mm);
            uprightSet.add(startX_mm + item.width_mm);
        }
    });
    const uprightPositions_mm = [...uprightSet].sort((a, b) => a - b);

    // Build a map: upright_mm → tallest adjacent module height
    const uprightHeight = {};
    itemGeos.forEach(({ item, startX_mm }) => {
        if (item._type !== 'module') return;
        [startX_mm, startX_mm + item.width_mm].forEach(pos => {
            if (!uprightHeight[pos] || item.height_mm > uprightHeight[pos].h) {
                uprightHeight[pos] = { h: item.height_mm, topY: wallY + (floorMm - FOOT_H - item.height_mm) * scale };
            }
        });
    });

    // ── Draw shelves (behind uprights) ──
    let bayNumber = 0;
    itemGeos.forEach(({ item, px }) => {
        if (item._type !== 'module') return;
        bayNumber++;
        const uW     = UPRIGHT_W * scale;
        const sT     = SHELF_T * scale;
        const bY     = wallY + (floorMm - FOOT_H - item.height_mm) * scale;
        const bH     = item.height_mm * scale;
        const innerW = (item.width_mm - UPRIGHT_W) * scale;
        const innerH = bH - sT;

        drawShelf(px + uW, bY, innerW);
        const gaps = item.shelves_count - 1;
        if (item.shelf_height_mm && gaps === 0) {
            // Workbench: draw work surface at the configured height from floor
            const shelfY = bY + bH - (item.shelf_height_mm * scale) - sT;
            drawShelf(px + uW, shelfY, innerW);
        } else if (gaps > 0) {
            const stepH = innerH / (gaps + 1);
            for (let s = 1; s <= gaps; s++) {
                drawShelf(px + uW, bY + sT + stepH * s - sT/2, innerW);
            }
        }
    });

    // ── Draw obstruction blocks (behind uprights, above floor) ──
    itemGeos.forEach(({ item, px, pw }) => {
        if (item._type !== 'obstruction') return;
        const obsH = floorMm * scale; // full wall height
        const obsY = wallY;
        el('rect', { x: px, y: obsY, width: pw, height: obsH, fill: 'url(#obs-hatch)', opacity: '0.55' }, svg);
        el('rect', { x: px, y: obsY, width: pw, height: obsH, fill: 'none', stroke: C.obsHatch, 'stroke-width': '1', opacity: '0.7' }, svg);
    });

    // ── Draw uprights on top ──
    uprightPositions_mm.forEach((pos_mm, idx) => {
        const info     = uprightHeight[pos_mm];
        if (!info) return;
        const isFirst  = idx === 0;
        const isLast   = idx === uprightPositions_mm.length - 1;
        drawUpright(wallX + pos_mm * scale, info.topY, info.h, isFirst || isLast);
    });

    // ── Labels ──
    bayNumber = 0;
    itemGeos.forEach(({ item, px, pw }) => {
        const dimY = wallY + wallH + 16;

        if (item._type === 'module') {
            bayNumber++;
            const uW     = UPRIGHT_W * scale;
            const sT     = SHELF_T * scale;
            const bY     = wallY + (floorMm - FOOT_H - item.height_mm) * scale;
            const bH     = item.height_mm * scale;
            const innerW = (item.width_mm - UPRIGHT_W) * scale;
            const innerH = bH - sT;

            // Bay label above
            const labelCX = px + uW + innerW / 2;
            el('rect', { x: labelCX-32, y: 3, width: 64, height: 19, fill: C.labelBg, rx: 4 }, svg);
            txt(`Bay ${bayNumber} · ${item.width_mm}×${item.height_mm}mm · ${item.shelves_count}T`,
                { x: labelCX, y: 15, 'text-anchor':'middle', 'font-family':'Inter,sans-serif', 'font-size':'8.5', 'font-weight':'900', fill: C.label }, svg);

            // Shelf spacing labels
            const gaps      = item.shelves_count - 1;
            const slots     = gaps + 1;
            const stepH     = innerH / slots;
            const spacingMm = Math.round((item.height_mm - SHELF_T * item.shelves_count) / slots);
            const spacingLX = px + uW + innerW + 4;
            for (let s = 0; s < slots; s++) {
                const slotTopY    = bY + sT + stepH * s;
                const slotCenterY = slotTopY + stepH / 2;
                if (stepH > 10) {
                    el('line', { x1: spacingLX, y1: slotTopY+sT/2, x2: spacingLX+4, y2: slotCenterY, stroke: C.spacing, 'stroke-width': '0.6', opacity:'0.6' }, svg);
                    el('line', { x1: spacingLX, y1: slotTopY+stepH-sT/2, x2: spacingLX+4, y2: slotCenterY, stroke: C.spacing, 'stroke-width': '0.6', opacity:'0.6' }, svg);
                    txt(`${spacingMm}mm`, { x: spacingLX+6, y: slotCenterY+3, 'font-family':'Inter,sans-serif', 'font-size':'7', 'font-weight':'700', fill: C.spacing, opacity:'0.85' }, svg);
                }
            }

            // Width dimension below
            const dimX1 = px + UPRIGHT_W*scale*0.5;
            const dimX2 = px + uW + innerW + UPRIGHT_W*scale*0.5;
            el('line', { x1:dimX1, y1:dimY, x2:dimX2, y2:dimY, stroke:C.dim, 'stroke-width':'0.8', 'stroke-dasharray':'3,2' }, svg);
            el('line', { x1:dimX1, y1:dimY-4, x2:dimX1, y2:dimY+4, stroke:C.dim, 'stroke-width':'1' }, svg);
            el('line', { x1:dimX2, y1:dimY-4, x2:dimX2, y2:dimY+4, stroke:C.dim, 'stroke-width':'1' }, svg);
            txt(`${item.width_mm}mm`, { x: (dimX1+dimX2)/2, y: dimY+11, 'text-anchor':'middle', 'font-family':'Inter,sans-serif', 'font-size':'8', fill: C.dim }, svg);

        } else {
            // Obstruction label above
            const labelCX = px + pw / 2;
            el('rect', { x: labelCX-24, y: 3, width: 48, height: 19, fill: '#1e0a0a', rx: 4 }, svg);
            txt(`${item.label} · ${item.width_mm}mm`,
                { x: labelCX, y: 15, 'text-anchor':'middle', 'font-family':'Inter,sans-serif', 'font-size':'8', 'font-weight':'900', fill: C.obsLabel }, svg);

            // Width dimension below
            el('line', { x1:px, y1:dimY, x2:px+pw, y2:dimY, stroke:C.obsDim, 'stroke-width':'0.8', 'stroke-dasharray':'3,2' }, svg);
            el('line', { x1:px, y1:dimY-4, x2:px, y2:dimY+4, stroke:C.obsDim, 'stroke-width':'1' }, svg);
            el('line', { x1:px+pw, y1:dimY-4, x2:px+pw, y2:dimY+4, stroke:C.obsDim, 'stroke-width':'1' }, svg);
            txt(`${item.width_mm}mm`, { x: px+pw/2, y: dimY+11, 'text-anchor':'middle', 'font-family':'Inter,sans-serif', 'font-size':'8', fill: C.obsDim }, svg);
        }
    });

    canvas.appendChild(svg);
}

// ── Edit Job State ──
let editingJobId = null;

// Cancel modal and reset edit state
function cancelModalAndReset() {
    editingJobId = null;
    toggleElement('customer-intake-modal');
    // Reset modal to new-job mode
    const banner = document.getElementById('edit-mode-banner');
    const title  = document.getElementById('intake-modal-title');
    const btnContinue = document.getElementById('modal-btn-continue');
    const btnInvoice  = document.getElementById('modal-btn-invoice');
    if (banner)      banner.classList.add('hidden');
    if (title)       title.textContent = 'Customer Intake & Delivery Booking';
    if (btnContinue) btnContinue.textContent = 'Confirm & Continue';
    if (btnInvoice)  btnInvoice.textContent  = 'Confirm & Send Invoice';
}

// Open the booking modal pre-populated with an existing job's data for editing
function openJobForEditing(jobId) {
    const job = liveJobsPipeline.find(j => j.id === jobId);
    if (!job) return;

    editingJobId = jobId;

    // Parse items
    let items = [];
    try {
        items = typeof job.job_items === 'string' ? JSON.parse(job.job_items) : (job.job_items || []);
    } catch(e) { items = []; }

    // Ensure every item has a cart_item_uuid
    editJobItems = items.map(i => ({ ...i, cart_item_uuid: i.cart_item_uuid || crypto.randomUUID() }));

    // Strip the phone/email suffix stored in address field
    const rawAddress  = (job.installation_address || '').split(' | Phone/Email:')[0].trim();
    const contactMatch = (job.installation_address || '').match(/Phone\/Email: \[(.+?)\]/);
    const contact = contactMatch ? contactMatch[1] : (job.customer_contact || '');

    // Populate edit modal fields
    document.getElementById('edit-job-name').value    = job.customer_name || '';
    document.getElementById('edit-job-contact').value = contact;
    document.getElementById('edit-job-address').value = rawAddress;
    document.getElementById('edit-job-title').textContent = `Edit Job #100${job.quote_number} — ${job.customer_name}`;

    // Install date
    if (job.install_date) {
        const datePart = job.install_date.split(' at ')[0];
        const timePart = (job.install_date.split(' at ')[1] || '').trim();
        editJobDateStr = datePart;
        editJobTimeStr = timePart;
        document.getElementById('edit-job-date-label').textContent = `${datePart} at ${timePart}`;
    } else {
        editJobDateStr = '';
        editJobTimeStr = '';
        document.getElementById('edit-job-date-label').textContent = 'No date set — pick one below';
    }

    renderEditJobModuleList();
    renderEditJobCalendar();
    renderEditModalTimeSlots(editJobDateStr);

    document.getElementById('edit-job-modal').classList.remove('hidden');
}

// Trigger Customer Intake Modal Pop-Up and Initialize Dynamic State Parameters
function lockAndConfirmBuildStructure() {
    if (activeBuildModulesListArray.length === 0) return alert("Add at least one modular package item to the configuration matrix.");
    
    editingJobId = null; // ensure we're in new-job mode
    document.getElementById('modal-summary-price-tag').textContent = `£${calculatedOrderTotalGrossValue.toFixed(2)}`;
    
    const baseNowDate = new Date();
    currentModalYear = baseNowDate.getFullYear();
    currentModalMonth = baseNowDate.getMonth();
    
    selectedModalIntakeDateStr = "";
    selectedModalIntakeTimeStr = "";

    // Reset button labels
    const btnContinue = document.getElementById('modal-btn-continue');
    const btnInvoice  = document.getElementById('modal-btn-invoice');
    const banner      = document.getElementById('edit-mode-banner');
    const title       = document.getElementById('intake-modal-title');
    if (btnContinue) btnContinue.textContent = 'Confirm & Continue';
    if (btnInvoice)  btnInvoice.textContent  = 'Confirm & Send Invoice';
    if (banner)      banner.classList.add('hidden');
    if (title)       title.textContent = 'Customer Intake & Delivery Booking';

    toggleElement('customer-intake-modal');
    buildModalInteractiveCalendarGrid();

    // Duration badge + reset slot grid
    const durationHours = calculateInstallDurationHours(activeBuildModulesListArray);
    const badge = document.getElementById('modal-duration-estimate');
    if (badge) {
        if (durationHours > 0) {
            const need = getSlotsNeededForDuration(durationHours);
            badge.textContent = `Est. ${formatDurationLabel(durationHours)} · ${need} slot${need !== 1 ? 's' : ''}`;
            badge.classList.remove('hidden');
        } else {
            badge.classList.add('hidden');
        }
    }
    renderModalTimeSlots(selectedModalIntakeDateStr);
}

// Change Viewed Month inside the Customer Intake Modal Calendar
function changeModalMonth(direction) {
    currentModalMonth += direction;
    if (currentModalMonth > 11) {
        currentModalMonth = 0;
        currentModalYear += 1;
    } else if (currentModalMonth < 0) {
        currentModalMonth = 11;
        currentModalYear -= 1;
    }
    buildModalInteractiveCalendarGrid();
}

// Build out a complete clean 7-column calendar grid for specified full months
function buildModalInteractiveCalendarGrid() {
    const grid = document.getElementById('modal-interactive-calendar-grid');
    if (!grid) return;

    const monthNames = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"];
    document.getElementById('modal-calendar-month-label').textContent = `${monthNames[currentModalMonth]} ${currentModalYear}`;

    const weekdayLabels = ['M', 'T', 'W', 'T', 'F', 'S', 'S'];
    let gridHTML = weekdayLabels.map(w => `<div class="text-slate-500 font-bold text-[10px] pb-1 border-b border-slate-800">${w}</div>`).join('');

    const firstDayIndex = new Date(currentModalYear, currentModalMonth, 1).getDay();
    const totalDays = new Date(currentModalYear, currentModalMonth + 1, 0).getDate();
    const startingSpacerCount = firstDayIndex === 0 ? 6 : firstDayIndex - 1;
    const blockedDates    = getBlockedDates();
    const availWeekdays   = getAvailableWeekdays();
    const configuredSlots = getAvailableTimeSlots();

    for (let i = 0; i < startingSpacerCount; i++) {
        gridHTML += `<div class="p-2 opacity-10"></div>`;
    }

    for (let day = 1; day <= totalDays; day++) {
        const dateObj    = new Date(currentModalYear, currentModalMonth, day);
        const iso        = `${currentModalYear}-${String(currentModalMonth + 1).padStart(2, '0')}-${String(day).padStart(2, '0')}`;
        const dow        = dateObj.getDay();
        const isSelected = iso === selectedModalIntakeDateStr;
        const isBlocked  = blockedDates.includes(iso);
        const isDayAvail = availWeekdays.has(dow);
        const isInstallDay = liveJobsPipeline.some(j => j.install_date && j.install_date.startsWith(iso) && j.status !== 'Settled' && j.status !== 'Cancelled');

        // Jobs already on this day (active, non-cancelled)
        const dayJobs = liveJobsPipeline.filter(j => j.install_date && j.install_date.startsWith(iso) && j.status !== 'Cancelled');

        // Capacity — count booked configured slots (isSlotBooked now handles both timed
        // and timeless jobs, so no separate untimed-count addition needed)
        let bookedCount = 0;
        let capacityDot = '';
        let isFullyBooked = false;
        if (!isBlocked && isDayAvail && configuredSlots.length > 0) {
            bookedCount = configuredSlots.filter(s => isSlotBooked(iso, s)).length;
            isFullyBooked = bookedCount >= configuredSlots.length;
            if (isFullyBooked) {
                capacityDot = `<span class="w-1.5 h-1.5 bg-rose-500 rounded-full block mt-0.5"></span>`;
            } else if (bookedCount > 0) {
                capacityDot = `<span class="w-1 h-1 bg-amber-400 rounded-full block mt-0.5"></span>`;
            } else if (isInstallDay) {
                capacityDot = `<span class="w-1 h-1 bg-brand-500 rounded-full block mt-0.5"></span>`;
            }
        }

        // Tooltip listing existing bookings on this day
        const dayJobsTooltip = dayJobs.length > 0
            ? dayJobs.map(j => {
                const t = j.install_date.includes(' at ') ? j.install_date.split(' at ')[1] : '';
                return `${t ? t + ' ' : ''}${j.customer_name}`;
              }).join('\n')
            : '';

        if (!isDayAvail && !isBlocked) {
            // Unavailable weekday — greyed out, not clickable
            gridHTML += `<div class="bg-slate-950/40 text-slate-700 border border-slate-900/60 rounded p-2 text-center opacity-40 pointer-events-none flex flex-col justify-center items-center min-h-[38px]">
                <span>${day}</span></div>`;
        } else if (isBlocked) {
            gridHTML += `<div class="bg-rose-950/30 text-rose-700 border border-rose-900/40 rounded p-2 text-center opacity-60 pointer-events-none flex flex-col justify-center items-center min-h-[38px]" title="Blocked">
                <span class="line-through">${day}</span>
                <span class="text-[7px] text-rose-700 font-black uppercase tracking-tighter">Blocked</span>
            </div>`;
        } else if (isFullyBooked) {
            // All slots taken — prevent selection to block double-booking
            gridHTML += `<div class="bg-rose-950/20 text-rose-700 border border-rose-900/40 rounded p-2 text-center opacity-70 pointer-events-none flex flex-col justify-center items-center min-h-[38px]" title="Fully booked&#10;${dayJobsTooltip}">
                <span class="text-slate-500">${day}</span>
                <span class="text-[7px] text-rose-600 font-black uppercase tracking-tighter">Full</span>
            </div>`;
        } else {
            const tooltipAttr = dayJobsTooltip ? ` title="${dayJobsTooltip}"` : '';
            const cls = isSelected
                ? "bg-brand-500/20 text-brand-400 border-2 border-brand-500 font-bold rounded p-2 cursor-pointer flex flex-col justify-between items-center min-h-[38px]"
                : isInstallDay
                    ? "bg-brand-500/5 text-brand-500/70 border border-brand-500/20 rounded p-2 cursor-pointer hover:border-brand-500 flex flex-col justify-between items-center min-h-[38px]"
                    : "bg-slate-900 text-slate-300 border border-slate-800 cursor-pointer hover:border-brand-500 rounded p-2 transition-all flex flex-col justify-between items-center min-h-[38px]";
            gridHTML += `<div onclick="setIntakeBookingDate(this, '${iso}')" id="modal-date-cell-${iso}" class="${cls}"${tooltipAttr}>
                <span>${day}</span>${capacityDot}
            </div>`;
        }
    }
    grid.innerHTML = gridHTML;
}

// Handle Day Selection inside Custom Intake Sheet Calendar
function setIntakeBookingDate(clickedElement, isoDateString) {
    document.querySelectorAll('[id^="modal-date-cell-"]').forEach(el => {
        el.classList.remove('border-brand-500', 'border-2', 'bg-brand-500/20', 'text-brand-400');
        if (el.title.includes("Saturday")) {
            el.className = "bg-rose-950/20 text-rose-400 border border-rose-900/40 font-bold rounded p-2 cursor-pointer hover:border-brand-500 flex flex-col justify-between items-center min-h-[38px]";
        } else if (el.querySelector('.bg-brand-500')) {
            el.className = "bg-brand-500/5 text-brand-500/70 border border-brand-500/20 rounded p-2 cursor-pointer hover:border-brand-500 flex flex-col justify-between items-center min-h-[38px]";
        } else {
            el.className = "bg-slate-900 text-slate-300 border border-slate-800 cursor-pointer hover:border-brand-500 rounded p-2 transition-all flex flex-col justify-between items-center min-h-[38px]";
        }
    });

    clickedElement.className = "bg-brand-500/20 text-brand-400 border-2 border-brand-500 font-bold rounded p-2 cursor-pointer flex flex-col justify-between items-center min-h-[38px]";
    selectedModalIntakeDateStr = isoDateString;
    selectedModalIntakeTimeStr = '';       // reset time — slots may differ per day
    renderModalTimeSlots(isoDateString);
    renderModalDayJobsPanel(isoDateString);
}

// Render the "already booked this day" panel beneath the slot picker
function renderModalDayJobsPanel(dateStr) {
    const panel = document.getElementById('modal-day-jobs-panel');
    const list  = document.getElementById('modal-day-jobs-list');
    if (!panel || !list) return;

    const dayJobs = liveJobsPipeline.filter(j =>
        j.install_date && j.install_date.startsWith(dateStr) &&
        j.status !== 'Cancelled'
    );

    if (dayJobs.length === 0) {
        panel.classList.add('hidden');
        return;
    }

    panel.classList.remove('hidden');
    list.innerHTML = dayJobs.map(job => {
        // Time label
        const timePart = job.install_date.includes(' at ') ? job.install_date.split(' at ')[1].trim() : null;
        const timeLabel = timePart ? formatSlotLabel(toHHMM(timePart)) : 'No time set';

        // Build summary — parse job_items
        let items = [];
        try { items = typeof job.job_items === 'string' ? JSON.parse(job.job_items) : (job.job_items || []); } catch(e) {}
        const buildStr = items.length
            ? items.map(i => `${i.name}${i.shelves_count ? ' (' + i.shelves_count + ' shelves)' : ''}`).join(', ')
            : 'No modules';

        return `
            <div class="flex items-start gap-2 bg-slate-900 border border-slate-800 rounded-lg px-2.5 py-2 text-[11px]">
                <span class="text-brand-400 font-black shrink-0">#100${job.quote_number}</span>
                <span class="text-slate-500 shrink-0">${timeLabel}</span>
                <span class="text-slate-300 truncate flex-1" title="${buildStr}">${buildStr}</span>
            </div>`;
    }).join('');
}

// Handle Arrival Window Time Slot Selection via Button Matrix Click Runs
function setIntakeBookingTime(timeStringValue) {
    selectedModalIntakeTimeStr = timeStringValue;
    renderModalTimeSlots(selectedModalIntakeDateStr);
}

// Unified Intake Validation Processing and Output Route Handler Switching
async function processOrderFromModalWorkflow(shouldAutoTriggerInvoicePDF) {
    const customer_name = document.getElementById('pipeline-name').value || "";
    const contact_details = document.getElementById('pipeline-contact').value || "";
    const installation_address = document.getElementById('pipeline-address').value || "";
    const sketchup_link = document.getElementById('pipeline-sketchup').value || null;

    if (!customer_name || !contact_details || !installation_address) {
        return alert("Validation blocker: Capture client name, contact details, and site location before authorization.");
    }
    if (!selectedModalIntakeDateStr || !selectedModalIntakeTimeStr) {
        return alert("Schedule blocker: Directly select an available installation date and arrival slot window.");
    }

    // Hard conflict guard — exact slot check at save time
    {
        const conflict = findSchedulingConflict(selectedModalIntakeDateStr, selectedModalIntakeTimeStr, activeBuildModulesListArray, editingJobId);
        if (conflict === 'blocked')        return alert('That day is blocked out — choose a different date.');
        if (conflict === 'unavailable_day') return alert('That day is not available for bookings.');
        if (conflict === 'slot_booked')    return alert('That time slot is already booked — choose a different slot.');
    }

    const selectedDateObj = new Date(selectedModalIntakeDateStr);
    const dayIndex = selectedDateObj.getDay();
    const shiftToMondayIndexValue = dayIndex === 0 ? -6 : 1 - dayIndex;
    let targetMondayDate = new Date(selectedDateObj);
    targetMondayDate.setDate(selectedDateObj.getDate() + shiftToMondayIndexValue);
    const computed_prep_date = targetMondayDate.toISOString().split('T')[0];

    const net_profit = calculatedOrderTotalGrossValue * 0.55;
    const combined_arrival_install_timestamp = `${selectedModalIntakeDateStr} at ${selectedModalIntakeTimeStr}`;

    const payload = {
        customer_name,
        installation_address: `${installation_address} | Phone/Email: [${contact_details}]`,
        sketchup_link,
        prep_date: computed_prep_date,
        install_date: combined_arrival_install_timestamp,
        gross_revenue: calculatedOrderTotalGrossValue,
        net_profit,
        job_items: activeBuildModulesListArray,
    };

    // ── EDIT MODE — update existing record ──
    if (editingJobId) {
        const { error } = await supabaseClient.from('job_ledger').update(payload).eq('id', editingJobId);
        if (error) {
            alert('Update failed: ' + error.message);
            return;
        }

        const savedJobId = editingJobId;
        cancelModalAndReset();
        activeBuildModulesListArray = [];
        renderActiveStagedModulesListLayout();
        renderDynamicConfiguratorParametersControls();
        await syncStateWithDatabaseCluster();
        openActiveJobDrilldownActionView(savedJobId);
        showToastNotification('Job updated ✓');

        const updatedJob = liveJobsPipeline.find(j => j.id === savedJobId);
        if (updatedJob) showDepositQRModal(updatedJob);

        if (shouldAutoTriggerInvoicePDF) {
            triggerInvoiceDocumentPDFDownload(savedJobId);
        }
        return;
    }

    // ── NEW JOB MODE — insert ──
    payload.deposit_amount = 150.00;
    payload.deposit_paid   = false;
    payload.status         = 'Lead';
    payload.obstructions   = [];

    const { data, error } = await supabaseClient.from('job_ledger').insert([payload]).select();

    if (error) {
        alert(error.message);
    } else {
        cancelModalAndReset();
        activeBuildModulesListArray = [];
        document.getElementById('pipeline-name').value    = '';
        document.getElementById('pipeline-contact').value = '';
        document.getElementById('pipeline-address').value = '';
        document.getElementById('pipeline-sketchup').value = '';

        await syncStateWithDatabaseCluster();
        renderActiveStagedModulesListLayout();
        renderDynamicConfiguratorParametersControls();

        if (data && data.length > 0) {
            showDepositQRModal(data[0]);
        } else {
            showToastNotification('Job created ✓');
        }

        if (shouldAutoTriggerInvoicePDF && data && data.length > 0) {
            triggerInvoiceDocumentPDFDownload(data[0].id);
        }
    }
}

// Mutate Active UX Layout Viewing Segment on Outstanding Tab
function setOutstandingViewMode(viewMode) {
    currentOutstandingViewMode = viewMode;
    
    const btnList = document.getElementById('view-btn-list');
    const btnCal = document.getElementById('view-btn-calendar');
    const wrapperDeck = document.getElementById('outstanding-deck-wrapper');
    const wrapperCal = document.getElementById('outstanding-calendar-wrapper');

    if (viewMode === 'list') {
        btnList.className = "flex-1 md:flex-none px-4 py-2 rounded-lg bg-brand-500 text-slate-950 font-black uppercase tracking-wide transition-all";
        btnCal.className = "flex-1 md:flex-none px-4 py-2 rounded-lg text-slate-400 font-bold uppercase tracking-wide hover:text-white transition-all";
        wrapperDeck.classList.remove('hidden');
        wrapperCal.classList.add('hidden');
    } else {
        btnCal.className = "flex-1 md:flex-none px-4 py-2 rounded-lg bg-brand-500 text-slate-950 font-black uppercase tracking-wide transition-all";
        btnList.className = "flex-1 md:flex-none px-4 py-2 rounded-lg text-slate-400 font-bold uppercase tracking-wide hover:text-white transition-all";
        wrapperDeck.classList.add('hidden');
        wrapperCal.classList.remove('hidden');
    }
    
    renderOutstandingJobsMatrixRegistryDeck();
}

// Increment / Decrement Outstanding Ledger Calendar Months
function changeOutstandingCalMonth(direction) {
    outstandingCalMonth += direction;
    if (outstandingCalMonth > 11) {
        outstandingCalMonth = 0;
        outstandingCalYear += 1;
    } else if (outstandingCalMonth < 0) {
        outstandingCalMonth = 11;
        outstandingCalYear -= 1;
    }
    renderOutstandingJobsMatrixRegistryDeck();
}

// Mutate Active UX Filter Target for Outstanding Active Deck Elements
function setOutstandingJobFilter(filterMode) {
    currentOutstandingFilterMode = filterMode;
    const filters = ['all', 'deposit_due', 'deposit_paid', 'ready_prep', 'ready_install', 'historical'];
    
    filters.forEach(f => {
        const el = document.getElementById(`filter-btn-${f}`);
        if (!el) return;
        
        if (f === filterMode) {
            el.className = "px-4 py-2.5 rounded-xl bg-brand-500 text-slate-950 font-black uppercase tracking-wider transition-all";
        } else {
            el.className = "px-4 py-2.5 rounded-xl bg-slate-950 text-slate-400 border border-slate-800 font-bold uppercase tracking-wider hover:text-white transition-all";
        }
    });

    renderOutstandingJobsMatrixRegistryDeck();
}

// Clear the search input and re-render
function clearJobSearch() {
    const input = document.getElementById('outstanding-search-input');
    if (input) input.value = '';
    const clearBtn = document.getElementById('outstanding-search-clear');
    if (clearBtn) clearBtn.classList.add('hidden');
    renderOutstandingJobsMatrixRegistryDeck();
}

// Upgraded Core Render Dispatch Engine: Splits workflows cleanly down selected layout paths
function renderOutstandingJobsMatrixRegistryDeck() {
    let filteredJobs = [];

    if (currentOutstandingFilterMode === 'historical') {
        filteredJobs = liveJobsPipeline.filter(j => j.status === 'Settled' || j.status === 'Completed' || j.status === 'Cancelled');
    } else {
        filteredJobs = liveJobsPipeline.filter(j => j.status !== 'Settled' && j.status !== 'Completed' && j.status !== 'Cancelled');

        if (currentOutstandingFilterMode === 'deposit_due') {
            filteredJobs = filteredJobs.filter(j => !j.deposit_paid);
        } else if (currentOutstandingFilterMode === 'deposit_paid') {
            filteredJobs = filteredJobs.filter(j => j.deposit_paid);
        } else if (currentOutstandingFilterMode === 'ready_prep') {
            filteredJobs = filteredJobs.filter(j => j.status === 'Booked');
        } else if (currentOutstandingFilterMode === 'ready_install') {
            filteredJobs = filteredJobs.filter(j => j.status === 'Prepped');
        }
    }

    // Apply search filter across all modes
    const searchInput = document.getElementById('outstanding-search-input');
    const searchTerm  = searchInput ? searchInput.value.trim().toLowerCase() : '';
    const clearBtn    = document.getElementById('outstanding-search-clear');
    if (clearBtn) clearBtn.classList.toggle('hidden', !searchTerm);

    if (searchTerm) {
        filteredJobs = filteredJobs.filter(j =>
            (j.customer_name        || '').toLowerCase().includes(searchTerm) ||
            (j.installation_address || '').toLowerCase().includes(searchTerm) ||
            String(j.quote_number   || '').includes(searchTerm)
        );
    }

    if (currentOutstandingViewMode === 'calendar') {
        renderOutstandingJobsCalendarView(filteredJobs);
    } else {
        const deck = document.getElementById('outstanding-jobs-cards-list-deck');
        if (!deck) return;

        if (filteredJobs.length === 0) {
            deck.innerHTML = `<p class="text-slate-500 text-xs italic p-6 text-center bg-slate-950 rounded-xl border border-slate-900 w-full">No records found for this selection.</p>`;
            return;
        }

        deck.innerHTML = filteredJobs.map(job => {
            let functionalUrgencyBadgeHTML = "";
            let accentBarClass = "bg-slate-700";

            if (job.status === 'Cancelled') {
                functionalUrgencyBadgeHTML = `<span class="bg-rose-500/10 text-rose-400 text-[11px] px-2.5 py-1 rounded-lg font-black border border-rose-500/20 uppercase tracking-wide">✕ Cancelled</span>`;
                accentBarClass = "bg-rose-500";
            } else if (job.status === 'Settled' || job.status === 'Completed') {
                functionalUrgencyBadgeHTML = `<span class="bg-emerald-500/10 text-emerald-400 text-[11px] px-2.5 py-1 rounded-lg font-black border border-emerald-500/20 uppercase tracking-wide">✓ Settled</span>`;
                accentBarClass = "bg-emerald-500";
            } else if (!job.deposit_paid) {
                functionalUrgencyBadgeHTML = `<span class="bg-orange-500/10 text-brand-400 text-[11px] px-2.5 py-1 rounded-lg font-black border border-brand-500/20 uppercase tracking-wide">🚨 Deposit Due</span>`;
                accentBarClass = "bg-brand-500";
            } else if (job.status === 'Booked') {
                functionalUrgencyBadgeHTML = `<span class="bg-amber-500/10 text-amber-400 text-[11px] px-2.5 py-1 rounded-lg font-black border border-amber-500/20 uppercase tracking-wide">🔨 Ready to Prep</span>`;
                accentBarClass = "bg-amber-500";
            } else if (job.status === 'Prepped') {
                functionalUrgencyBadgeHTML = `<span class="bg-sky-500/10 text-sky-400 text-[11px] px-2.5 py-1 rounded-lg font-black border border-sky-500/20 uppercase tracking-wide">🚚 Install Ready</span>`;
                accentBarClass = "bg-sky-500";
            } else {
                functionalUrgencyBadgeHTML = `<span class="bg-slate-800 text-slate-400 text-[11px] px-2.5 py-1 rounded-lg font-bold uppercase tracking-wide">${job.status}</span>`;
            }

            const installDay = job.install_date ? job.install_date.split(' at ')[0] : 'No date';
            const installTime = job.install_date && job.install_date.includes(' at ') ? job.install_date.split(' at ')[1] : null;

            return `
                <div onclick="openActiveJobDrilldownActionView('${job.id}')"
                     id="job-card-${job.id}"
                     class="rounded-xl border border-slate-800 bg-slate-950 cursor-pointer hover:border-brand-500/60 active:scale-[0.99] transition-all shadow-md overflow-hidden flex group">
                    <div class="w-1.5 flex-shrink-0 ${accentBarClass} rounded-l-xl"></div>
                    <div class="flex-1 p-3.5 min-w-0">
                        <div class="flex justify-between items-start gap-2 mb-2.5">
                            <div class="min-w-0 flex-1">
                                <div class="flex items-baseline gap-2 flex-wrap">
                                    <span class="font-black text-white text-sm leading-tight">${job.customer_name}</span>
                                    <span class="text-brand-400 text-xs font-mono font-bold">#100${job.quote_number}</span>
                                </div>
                                <p class="text-xs text-slate-400 truncate mt-0.5">${job.installation_address}</p>
                            </div>
                            <span class="text-emerald-400 font-black text-base shrink-0">£${parseFloat(job.gross_revenue).toFixed(0)}</span>
                        </div>
                        <div class="flex justify-between items-center">
                            ${functionalUrgencyBadgeHTML}
                            <div class="flex items-center gap-2 text-xs text-slate-500">
                                ${job.notes ? `<span class="text-violet-400 text-[10px]" title="Has notes">📝</span>` : ''}
                                <span class="font-medium">${installDay}${installTime ? `<span class="text-slate-600 ml-1">@ ${installTime}</span>` : ''}</span>
                            </div>
                        </div>
                    </div>
                </div>
            `;
        }).join('');
    }
}

// Map and Build Out Live Filtered Pipeline Items inside Full Month Grid
function renderOutstandingJobsCalendarView(filteredJobsList) {
    const grid = document.getElementById('outstanding-tab-calendar-grid');
    if (!grid) return;

    const monthNames = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"];
    document.getElementById('outstanding-calendar-month-label').textContent = `${monthNames[outstandingCalMonth]} ${outstandingCalYear}`;

    const weekdayLabels = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'];
    let gridHTML = weekdayLabels.map(w => `<div class="text-slate-500 font-bold text-xs pb-2 border-b border-slate-800 uppercase">${w}</div>`).join('');
    
    const firstDayIndex = new Date(outstandingCalYear, outstandingCalMonth, 1).getDay(); 
    const totalDays = new Date(outstandingCalYear, outstandingCalMonth + 1, 0).getDate();
    const startingSpacerCount = firstDayIndex === 0 ? 6 : firstDayIndex - 1;

    for (let i = 0; i < startingSpacerCount; i++) {
        gridHTML += `<div class="p-2 bg-slate-950/20 border border-slate-900/30 min-h-[85px] rounded opacity-10"></div>`;
    }

    const blockedDatesOutCal = getBlockedDates();
    for (let day = 1; day <= totalDays; day++) {
        const dateObj = new Date(outstandingCalYear, outstandingCalMonth, day);
        const isoStringDateMarker = `${outstandingCalYear}-${String(outstandingCalMonth + 1).padStart(2, '0')}-${String(day).padStart(2, '0')}`;
        const dow = dateObj.getDay();
        const isBlocked = blockedDatesOutCal.includes(isoStringDateMarker);

        const dayMatchedJobs = filteredJobsList.filter(j => j.install_date && j.install_date.startsWith(isoStringDateMarker));

        let cellStylingTokens = "bg-slate-900/60 border border-slate-800 text-slate-300 p-2 rounded-xl text-left flex flex-col gap-1 min-h-[110px] transition-all";

        if (dow === 0) {
            cellStylingTokens = "bg-slate-950/40 border border-slate-900/60 text-slate-600 p-2 rounded-xl text-left opacity-30 pointer-events-none line-through min-h-[110px]";
        } else if (isBlocked) {
            cellStylingTokens = "bg-rose-950/30 border border-rose-900/40 text-rose-800 p-2 rounded-xl text-left opacity-60 pointer-events-none min-h-[110px]";
        } else if (dow === 6) {
            cellStylingTokens = "bg-rose-950/10 border border-rose-900/30 text-rose-400 p-2 rounded-xl text-left min-h-[110px]";
        } else if (dayMatchedJobs.length > 0) {
            cellStylingTokens = "bg-brand-500/5 border border-brand-500/30 text-slate-200 p-2 rounded-xl text-left min-h-[110px] shadow-lg";
        }

        const visibleJobs = dayMatchedJobs.slice(0, 3);
        const overflowCount = dayMatchedJobs.length - visibleJobs.length;

        let jobsBadgesMarkup = visibleJobs.map(job => {
            let badgeBgClass = "bg-brand-500 text-slate-950 font-black";
            if (job.status === 'Settled' || job.status === 'Completed') badgeBgClass = "bg-emerald-500 text-slate-950 font-bold";
            else if (!job.deposit_paid) badgeBgClass = "bg-orange-500 text-slate-950 font-bold animate-pulse";
            else if (job.status === 'Prepped') badgeBgClass = "bg-sky-500 text-slate-950 font-bold";

            const timePart = job.install_date.includes(' at ') ? job.install_date.split(' at ')[1].replace(':00', '').replace(' AM', 'am').replace(' PM', 'pm') : '';
            const firstName = job.customer_name ? job.customer_name.split(' ')[0] : `#100${job.quote_number}`;

            return `
                <button onclick="openActiveJobDrilldownActionView('${job.id}')" type="button" class="${badgeBgClass} text-[10px] px-1.5 py-1 rounded-lg w-full text-left border border-slate-950/50 block hover:scale-[1.02] transition-all" title="${job.customer_name} — ${job.installation_address}">
                    <span class="block font-black truncate">${firstName}</span>
                    <span class="block font-bold opacity-80">${timePart ? timePart + ' · ' : ''}#100${job.quote_number}</span>
                </button>
            `;
        }).join('');

        if (overflowCount > 0) {
            jobsBadgesMarkup += `<span class="text-[9px] text-slate-400 font-bold px-1 block">+${overflowCount} more</span>`;
        }

        const blockedLabel = isBlocked ? `<span class="text-[8px] font-black uppercase text-rose-700 tracking-tight">Blocked</span>` : '';

        gridHTML += `
            <div class="${cellStylingTokens}">
                <div class="flex items-center justify-between">
                    <span class="text-xs font-bold ${dayMatchedJobs.length > 0 ? 'text-brand-400' : isBlocked ? 'text-rose-800 line-through' : 'text-slate-500'}">${day}</span>
                    ${dayMatchedJobs.length > 1 ? `<span class="text-[9px] text-slate-500 font-bold">${dayMatchedJobs.length} jobs</span>` : ''}
                </div>
                ${blockedLabel}
                <div class="w-full space-y-1">
                    ${jobsBadgesMarkup}
                </div>
            </div>
        `;
    }
    grid.innerHTML = gridHTML;
}

// Display Specific Custom Slicing and Verification Guidelines for Clicked Job Card
function openActiveJobDrilldownActionView(jobId) {
    activeJobDrilldownId = jobId;
    const job = liveJobsPipeline.find(j => j.id === jobId);
    if (!job) return;

    // Highlight selected card
    document.querySelectorAll('[id^="job-card-"]').forEach(el => {
        el.classList.remove('border-brand-500', 'bg-slate-900/80');
        el.classList.add('border-slate-800', 'bg-slate-950');
    });
    const selectedCard = document.getElementById(`job-card-${jobId}`);
    if (selectedCard) {
        selectedCard.classList.remove('border-slate-800', 'bg-slate-950');
        selectedCard.classList.add('border-brand-500', 'bg-slate-900/80');
    }

    const viewport = document.getElementById('drilldown-action-task-viewport');
    let productItemsArray = [];
    try { productItemsArray = typeof job.job_items === 'string' ? JSON.parse(job.job_items) : job.job_items || []; } catch(e) { productItemsArray = []; }

    let groupedAssembliesHTML = productItemsArray.map((p, idx) => {
        const price = p.workingPrice != null ? `£${parseFloat(p.workingPrice).toFixed(2)}` : '';
        return `
        <div class="bg-slate-800 p-3 rounded-xl border border-slate-700 space-y-1.5">
            <div class="flex justify-between items-start gap-2">
                <span class="text-brand-400 font-black text-xs uppercase tracking-wider">Bay ${idx + 1}: ${p.name}</span>
                ${price ? `<span class="text-emerald-400 font-black text-xs whitespace-nowrap">${price}</span>` : ''}
            </div>
            <div class="grid grid-cols-2 gap-x-4 gap-y-1 text-xs text-slate-400">
                <div>Width: <strong class="text-white">${p.width_mm}mm</strong></div>
                <div>Depth: <strong class="text-white">${p.depth_mm}mm</strong></div>
                <div>Height: <strong class="text-white">${p.height_mm}mm</strong></div>
                <div>Shelf tiers: <strong class="text-white">${p.shelves_count}</strong></div>
                ${p.shelf_height_mm != null ? `<div class="col-span-2">Work surface: <strong class="text-white">${p.shelf_height_mm}mm from floor</strong></div>` : ''}
            </div>
        </div>`;
    }).join('');

    const statusNodesList = ['Lead', 'Quoted', 'Booked', 'Prepped', 'Installed', 'Settled'];
    const currentStatusIndex = statusNodesList.indexOf(job.status);

    let statusBadgeHTML = '';
    if (job.status === 'Cancelled') {
        statusBadgeHTML = `<span class="bg-rose-500/15 text-rose-400 text-[11px] px-2.5 py-1 rounded-lg font-black border border-rose-500/25 uppercase tracking-wide">✕ Cancelled</span>`;
    } else if (job.status === 'Settled' || job.status === 'Completed') {
        statusBadgeHTML = `<span class="bg-emerald-500/15 text-emerald-400 text-[11px] px-2.5 py-1 rounded-lg font-black border border-emerald-500/25 uppercase tracking-wide">✓ Settled</span>`;
    } else if (!job.deposit_paid) {
        statusBadgeHTML = `<span class="bg-brand-500/15 text-brand-400 text-[11px] px-2.5 py-1 rounded-lg font-black border border-brand-500/25 uppercase tracking-wide">🚨 Deposit Due</span>`;
    } else if (job.status === 'Booked') {
        statusBadgeHTML = `<span class="bg-amber-500/15 text-amber-400 text-[11px] px-2.5 py-1 rounded-lg font-black border border-amber-500/25 uppercase tracking-wide">🔨 Ready to Prep</span>`;
    } else if (job.status === 'Prepped') {
        statusBadgeHTML = `<span class="bg-sky-500/15 text-sky-400 text-[11px] px-2.5 py-1 rounded-lg font-black border border-sky-500/25 uppercase tracking-wide">🚚 Install Ready</span>`;
    } else {
        statusBadgeHTML = `<span class="bg-slate-700 text-slate-300 text-[11px] px-2.5 py-1 rounded-lg font-bold uppercase tracking-wide">${job.status}</span>`;
    }

    const pipelineStepperHTML = `
        <div class="flex bg-slate-950 p-1.5 rounded-xl border border-slate-900 gap-1 overflow-x-auto custom-scrollbar">
            ${statusNodesList.map((node, idx) => {
                let cls = "flex-1 px-2 py-2 rounded-lg text-[11px] font-bold uppercase tracking-tight transition-all active:scale-95 whitespace-nowrap text-center";
                if (idx === currentStatusIndex) {
                    cls += " bg-brand-500 text-slate-950 font-black shadow";
                } else if (idx < currentStatusIndex) {
                    cls += " text-emerald-400 bg-emerald-950/40 border border-emerald-900/30";
                } else {
                    cls += " text-slate-500 hover:text-slate-300 hover:bg-slate-800/60";
                }
                return `<button onclick="mutateJobPipelineNodeStateDirect('${job.id}', '${node}')" class="${cls}">${idx < currentStatusIndex ? '✓ ' : ''}${node}</button>`;
            }).join('')}
        </div>
    `;

    viewport.innerHTML = `
        <div class="flex flex-col h-full">

            <!-- Sticky Header -->
            <div class="flex-shrink-0 p-5 border-b border-slate-700 bg-slate-800/80 backdrop-blur-sm">
                <div class="flex justify-between items-start gap-3">
                    <div class="min-w-0 flex-1">
                        <div class="flex items-center gap-2 mb-2 flex-wrap">
                            <span class="text-[11px] bg-slate-900 border border-slate-700 px-2.5 py-0.5 rounded-lg text-slate-400 font-mono tracking-wide">#100${job.quote_number}</span>
                            ${statusBadgeHTML}
                        </div>
                        <h3 class="text-xl font-black text-white leading-tight">${job.customer_name}</h3>
                        <p class="text-xs text-slate-400 mt-0.5 truncate">${job.installation_address}</p>
                    </div>
                    <div class="text-right shrink-0">
                        <span class="text-2xl font-black text-emerald-400">£${parseFloat(job.gross_revenue).toFixed(0)}</span>
                        <p class="text-[10px] text-slate-500 uppercase tracking-widest mt-0.5">Revenue</p>
                    </div>
                </div>
            </div>

            <!-- Scrollable Body -->
            <div class="flex-1 overflow-y-auto custom-scrollbar p-5 space-y-6">

                <!-- 1. Pipeline Status Stepper -->
                <div>
                    <p class="text-[11px] font-black text-slate-500 uppercase tracking-widest mb-2">Pipeline Stage</p>
                    ${pipelineStepperHTML}
                </div>

                <!-- 2. Quick Actions — prominent, right at the top -->
                <div>
                    <p class="text-[11px] font-black text-slate-500 uppercase tracking-widest mb-3">Actions</p>
                    <div class="grid grid-cols-3 gap-2">
                        <button onclick="openJobForEditing('${job.id}')" class="flex flex-col items-center justify-center gap-1.5 bg-sky-500/10 border border-sky-500/25 text-sky-300 py-4 rounded-xl text-[11px] font-black uppercase hover:bg-sky-500/20 hover:border-sky-400 transition-all active:scale-95">
                            <span class="text-xl leading-none">✏️</span>Edit Job
                        </button>
                        <button onclick="showDepositQRForJob('${job.id}')" class="flex flex-col items-center justify-center gap-1.5 bg-brand-500/10 border border-brand-500/25 text-brand-400 py-4 rounded-xl text-[11px] font-black uppercase hover:bg-brand-500/20 hover:border-brand-500 transition-all active:scale-95">
                            <span class="text-xl leading-none">⬛</span>Deposit QR
                        </button>
                        <button onclick="cloneJobAsNewLead('${job.id}')" class="flex flex-col items-center justify-center gap-1.5 bg-violet-500/10 border border-violet-500/25 text-violet-300 py-4 rounded-xl text-[11px] font-black uppercase hover:bg-violet-500/20 hover:border-violet-400 transition-all active:scale-95">
                            <span class="text-xl leading-none">⧉</span>Clone Job
                        </button>
                        <button onclick="toggleDepositStatusPaidRegistryFlag('${job.id}', ${job.deposit_paid})" class="flex flex-col items-center justify-center gap-1.5 py-4 rounded-xl text-[11px] font-black uppercase transition-all active:scale-95 ${job.deposit_paid ? 'bg-emerald-500/10 border border-emerald-500/25 text-emerald-300 hover:bg-emerald-500/20 hover:border-emerald-400' : 'bg-orange-500/10 border border-orange-500/25 text-orange-300 hover:bg-orange-500/20 hover:border-orange-400'}">
                            <span class="text-xl leading-none">${job.deposit_paid ? '✓' : '💰'}</span>${job.deposit_paid ? 'Unmark Paid' : 'Mark Paid'}
                        </button>
                        <button onclick="triggerInvoiceDocumentPDFDownload('${job.id}')" class="flex flex-col items-center justify-center gap-1.5 bg-slate-900 border border-slate-700 text-slate-300 py-4 rounded-xl text-[11px] font-bold uppercase hover:border-brand-500 hover:text-brand-400 transition-all active:scale-95">
                            <span class="text-xl leading-none">📄</span>Print PDF
                        </button>
                        ${job.status !== 'Cancelled' ? `
                        <button onclick="cancelJob('${job.id}')" class="flex flex-col items-center justify-center gap-1.5 bg-rose-500/10 border border-rose-500/25 text-rose-400 py-4 rounded-xl text-[11px] font-bold uppercase hover:bg-rose-500/20 hover:border-rose-400 transition-all active:scale-95">
                            <span class="text-xl leading-none">✕</span>Cancel
                        </button>` : '<div></div>'}
                    </div>
                </div>

                <!-- 3. Financial & Schedule Details -->
                <div>
                    <p class="text-[11px] font-black text-slate-500 uppercase tracking-widest mb-3">Financial & Schedule</p>
                    <div class="grid grid-cols-2 gap-2.5">
                        <div class="bg-slate-950 p-3.5 rounded-xl border border-slate-900 space-y-1">
                            <span class="text-[10px] text-slate-600 uppercase tracking-widest block">Deposit £150</span>
                            <span class="font-black text-sm ${job.deposit_paid ? 'text-emerald-400' : 'text-rose-400'}">${job.deposit_paid ? '✓ SECURED' : '⚠ OUTSTANDING'}</span>
                        </div>
                        <div class="bg-slate-950 p-3.5 rounded-xl border border-slate-900 space-y-1">
                            <span class="text-[10px] text-slate-600 uppercase tracking-widest block">Gross Revenue</span>
                            <span class="font-black text-sm text-emerald-400">£${parseFloat(job.gross_revenue).toFixed(0)}</span>
                        </div>
                        <div class="bg-slate-950 p-3.5 rounded-xl border border-slate-900 space-y-1">
                            <span class="text-[10px] text-slate-600 uppercase tracking-widest block">Install Date</span>
                            <span class="font-bold text-xs text-slate-200">${job.install_date || 'TBD'}</span>
                        </div>
                        <div class="bg-slate-950 p-3.5 rounded-xl border border-slate-900 space-y-1">
                            <span class="text-[10px] text-slate-600 uppercase tracking-widest block">Prep Week</span>
                            <span class="font-bold text-xs text-slate-200">${job.prep_date || 'TBD'}</span>
                        </div>
                    </div>
                </div>

                <!-- 4. Structural Components -->
                ${productItemsArray.length > 0 ? `
                <div>
                    <p class="text-[11px] font-black text-slate-500 uppercase tracking-widest mb-3">Structural Components</p>
                    <div class="grid grid-cols-1 md:grid-cols-2 gap-2">${groupedAssembliesHTML}</div>
                </div>
                ` : ''}

                ${job.sketchup_link ? `
                <a href="${job.sketchup_link}" target="_blank" class="flex items-center gap-2.5 bg-slate-950 border border-slate-800 text-sky-400 p-3.5 rounded-xl text-xs font-bold hover:border-sky-500/50 hover:bg-sky-500/5 transition-all">
                    🔗 <span>Open SketchUp Model</span>
                </a>
                ` : ''}

                <!-- 5. Notes — last, least urgent -->
                <div class="space-y-2.5">
                    <div class="flex justify-between items-center">
                        <p class="text-[11px] font-black text-slate-500 uppercase tracking-widest">Site & Customer Notes</p>
                        ${job.notes_updated_at ? `<span class="text-[10px] text-slate-600 italic">${job.notes_updated_at}</span>` : ''}
                    </div>
                    <textarea
                        id="job-notes-input-${job.id}"
                        rows="4"
                        placeholder="Access, parking, pets, preferences, site quirks, contact preference…"
                        class="w-full bg-slate-900 border border-slate-700 rounded-xl p-3 text-white text-xs outline-none focus:border-violet-500 resize-none leading-relaxed placeholder:text-slate-600 transition-all"
                    >${job.notes || ''}</textarea>
                    <button onclick="saveJobNotes('${job.id}')" class="w-full bg-violet-500/10 border border-violet-500/30 text-violet-300 font-black py-2.5 rounded-xl text-xs uppercase tracking-widest hover:bg-violet-500/20 hover:border-violet-400 transition-all">
                        Save Notes
                    </button>
                </div>

            </div>
        </div>
    `;
}

// Save notes for a job and refresh the drilldown panel
async function saveJobNotes(jobId) {
    const textarea = document.getElementById(`job-notes-input-${jobId}`);
    if (!textarea) return;

    const notes = textarea.value.trim();
    const timestamp = new Date().toLocaleDateString('en-GB', { day: '2-digit', month: 'short', year: 'numeric', hour: '2-digit', minute: '2-digit' });

    const { error } = await supabaseClient
        .from('job_ledger')
        .update({ notes, notes_updated_at: timestamp })
        .eq('id', jobId);

    if (error) {
        alert('Notes save failed: ' + error.message);
        return;
    }

    // Update local state so re-opening the drilldown reflects the change without a full sync
    const localJob = liveJobsPipeline.find(j => j.id === jobId);
    if (localJob) {
        localJob.notes = notes;
        localJob.notes_updated_at = timestamp;
    }

    showToastNotification('Notes saved ✓');
}

// ── Edit Job Modal — isolated state, never touches the main configurator ──
let editJobItems   = [];
let editJobDateStr = '';
let editJobTimeStr = '';
let editCalYear    = new Date().getFullYear();
let editCalMonth   = new Date().getMonth();

function closeEditJobModal() {
    document.getElementById('edit-job-modal').classList.add('hidden');
    editingJobId   = null;
    editJobItems   = [];
    editJobDateStr = '';
    editJobTimeStr = '';
}

function setEditJobTime(time) {
    editJobTimeStr = time;
    updateEditJobDateLabel();
    renderEditModalTimeSlots(editJobDateStr);
}

function updateEditJobDateLabel() {
    const label = document.getElementById('edit-job-date-label');
    if (!label) return;
    if (editJobDateStr && editJobTimeStr) {
        label.textContent = `${editJobDateStr} at ${editJobTimeStr}`;
        label.className = 'text-xs font-bold text-emerald-400';
    } else if (editJobDateStr) {
        label.textContent = `${editJobDateStr} — pick a time slot`;
        label.className = 'text-xs font-bold text-amber-400';
    } else {
        label.textContent = 'Select a date below';
        label.className = 'text-xs font-bold text-slate-500';
    }
}

function changeEditCalMonth(dir) {
    editCalMonth += dir;
    if (editCalMonth > 11) { editCalMonth = 0; editCalYear++; }
    if (editCalMonth < 0)  { editCalMonth = 11; editCalYear--; }
    renderEditJobCalendar();
}

function renderEditJobCalendar() {
    const grid  = document.getElementById('edit-job-calendar-grid');
    const label = document.getElementById('edit-cal-month-label');
    if (!grid) return;

    const monthNames = ['January','February','March','April','May','June','July','August','September','October','November','December'];
    if (label) label.textContent = `${monthNames[editCalMonth]} ${editCalYear}`;

    const firstDay  = new Date(editCalYear, editCalMonth, 1).getDay();
    const daysInMonth = new Date(editCalYear, editCalMonth + 1, 0).getDate();
    const today = new Date();
    const dayHeaders = ['Su','Mo','Tu','We','Th','Fr','Sa'];

    let html = dayHeaders.map(d => `<div class="text-[10px] font-bold text-slate-600 text-center py-1">${d}</div>`).join('');
    for (let i = 0; i < firstDay; i++) html += `<div></div>`;

    const blockedDates  = getBlockedDates();
    const availWeekdays = getAvailableWeekdays();
    for (let d = 1; d <= daysInMonth; d++) {
        const dateStr   = `${editCalYear}-${String(editCalMonth+1).padStart(2,'0')}-${String(d).padStart(2,'0')}`;
        const dow       = new Date(editCalYear, editCalMonth, d).getDay();
        const isPast    = new Date(editCalYear, editCalMonth, d) < new Date(today.getFullYear(), today.getMonth(), today.getDate());
        const isSelected  = dateStr === editJobDateStr;
        const isBlocked   = blockedDates.includes(dateStr);
        const isDayAvail  = availWeekdays.has(dow);

        let cls = "text-xs rounded-lg py-1.5 text-center font-bold transition-all ";
        let onclick = '';
        if (isSelected)           cls += "bg-brand-500 text-slate-950 shadow";
        else if (isBlocked)       cls += "text-rose-800 bg-rose-950/30 line-through cursor-not-allowed opacity-50";
        else if (!isDayAvail)     cls += "text-slate-700 cursor-not-allowed opacity-30";
        else if (isPast)          cls += "text-slate-700 cursor-not-allowed";
        else                      cls += "text-slate-400 cursor-pointer hover:bg-slate-800";

        if (!isPast && !isBlocked && isDayAvail) onclick = `onclick="selectEditJobDate('${dateStr}')"`;
        html += `<div class="${cls}" ${onclick}>${d}</div>`;
    }

    grid.innerHTML = html;
}

function selectEditJobDate(dateStr) {
    editJobDateStr = dateStr;
    editJobTimeStr = '';          // reset time when date changes
    renderEditJobCalendar();
    updateEditJobDateLabel();
    renderEditModalTimeSlots(dateStr);
}

// Edit modal module management — mirrors main configurator but uses editJobItems
function addEditJobModule() {
    const dropdown = document.getElementById('edit-module-dropdown');
    if (!dropdown || !dropdown.value) return;
    const item = dbProducts.find(p => p.id === dropdown.value);
    if (!item) return;
    editJobItems.push({
        ...item,
        cart_item_uuid: crypto.randomUUID(),
        workingPrice:  parseFloat(item.base_retail_price),
        width_mm:      parseInt(item.width_mm),
        depth_mm:      parseInt(item.depth_mm),
        height_mm:     parseInt(item.height_mm),
        shelves_count: (() => { const r = dbRecipes[item.id]; return parseInt(r != null && r.default_shelves != null ? r.default_shelves : (item.shelves_count || 4)); })()
    });
    renderEditJobModuleList();
}

function removeEditJobModule(uuid) {
    editJobItems = editJobItems.filter(i => i.cart_item_uuid !== uuid);
    renderEditJobModuleList();
}

function adjustEditJobShelfCount(uuid, delta) {
    const item = editJobItems.find(i => i.cart_item_uuid === uuid);
    if (!item) return;
    item.shelves_count = Math.max(1, Math.min(10, item.shelves_count + delta));
    const baseTemplate = dbProducts.find(p => p.id === item.id);
    const defaultShelves = baseTemplate ? parseInt(baseTemplate.shelves_count || 4) : 4;
    const extraShelfCost = parseFloat(dbConfigs['shelf_tier_modifier_price'] || 15.00);
    item.workingPrice = parseFloat(item.base_retail_price) + ((item.shelves_count - defaultShelves) * extraShelfCost);
    renderEditJobModuleList();
}

function renderEditJobModuleList() {
    const container = document.getElementById('edit-job-module-list');
    if (!container) return;

    // Recalculate total
    let total = 0;
    editJobItems.forEach(item => {
        const baseTemplate = dbProducts.find(p => p.id === item.id);
        const defaultShelves = baseTemplate ? parseInt(baseTemplate.shelves_count || 4) : 4;
        const extraShelfCost = parseFloat(dbConfigs['shelf_tier_modifier_price'] || 15.00);
        item.workingPrice = parseFloat(item.base_retail_price) + ((item.shelves_count - defaultShelves) * extraShelfCost);
        total += item.workingPrice;
    });

    const priceEl = document.getElementById('edit-job-total-price');
    if (priceEl) priceEl.textContent = `£${total.toFixed(2)}`;

    if (editJobItems.length === 0) {
        container.innerHTML = `<p class="text-slate-500 text-xs italic p-3 text-center">No modules — add one above.</p>`;
        return;
    }

    container.innerHTML = editJobItems.map((item, idx) => `
        <div class="flex items-center justify-between bg-slate-900 p-2.5 rounded-xl border border-slate-800 text-xs gap-2">
            <span class="text-slate-200 font-bold truncate flex-1">Bay ${idx+1}: ${item.name}</span>
            <div class="flex items-center gap-1.5 shrink-0">
                <button onclick="adjustEditJobShelfCount('${item.cart_item_uuid}',-1)" class="w-6 h-6 bg-slate-800 text-white font-black rounded hover:bg-brand-500 hover:text-slate-950 transition-all flex items-center justify-center">-</button>
                <span class="text-brand-400 font-black w-4 text-center">${item.shelves_count}</span>
                <button onclick="adjustEditJobShelfCount('${item.cart_item_uuid}',1)" class="w-6 h-6 bg-slate-800 text-white font-black rounded hover:bg-brand-500 hover:text-slate-950 transition-all flex items-center justify-center">+</button>
                <span class="text-slate-400 ml-1">£${item.workingPrice.toFixed(2)}</span>
                <button onclick="removeEditJobModule('${item.cart_item_uuid}')" class="text-slate-600 hover:text-rose-400 font-black text-sm leading-none ml-1 transition-all">×</button>
            </div>
        </div>
    `).join('');
}

async function saveEditedJob(reprintInvoice) {
    if (!editingJobId) return;
    const name    = document.getElementById('edit-job-name').value.trim();
    const contact = document.getElementById('edit-job-contact').value.trim();
    const address = document.getElementById('edit-job-address').value.trim();

    if (!name || !contact || !address) return alert('Name, contact and address are required.');
    if (editJobItems.length === 0) return alert('At least one module is required.');
    if (!editJobDateStr || !editJobTimeStr) return alert('Select an install date and time slot.');

    // Hard conflict guard — exact slot check at save time
    {
        const conflict = findSchedulingConflict(editJobDateStr, editJobTimeStr, editJobItems, editingJobId);
        if (conflict === 'blocked')        return alert('That day is blocked out — choose a different date.');
        if (conflict === 'unavailable_day') return alert('That day is not available for bookings.');
        if (conflict === 'slot_booked')    return alert('That time slot is already booked — choose a different slot.');
    }

    const selectedDateObj = new Date(editJobDateStr);
    const dayIndex = selectedDateObj.getDay();
    const shift = dayIndex === 0 ? -6 : 1 - dayIndex;
    const monday = new Date(selectedDateObj);
    monday.setDate(selectedDateObj.getDate() + shift);
    const prep_date = monday.toISOString().split('T')[0];

    let total = 0;
    editJobItems.forEach(i => total += i.workingPrice);

    const payload = {
        customer_name:        name,
        installation_address: `${address} | Phone/Email: [${contact}]`,
        install_date:         `${editJobDateStr} at ${editJobTimeStr}`,
        prep_date,
        gross_revenue:        total,
        net_profit:           total * 0.55,
        job_items:            editJobItems,
    };

    const { error } = await supabaseClient.from('job_ledger').update(payload).eq('id', editingJobId);
    if (error) { alert('Save failed: ' + error.message); return; }

    const savedId = editingJobId;
    closeEditJobModal();
    showToastNotification('Job updated ✓');
    await syncStateWithDatabaseCluster();
    openActiveJobDrilldownActionView(savedId);
    if (reprintInvoice) triggerInvoiceDocumentPDFDownload(savedId);
}

// Cancel a job — sets status to Cancelled after confirmation
async function cancelJob(jobId) {
    const job = liveJobsPipeline.find(j => j.id === jobId);
    if (!job) return;
    if (!confirm(`Cancel job #100${job.quote_number} — ${job.customer_name}?\n\nThis will mark the job as Cancelled. It will be moved to the historical view and excluded from active pipeline.`)) return;

    const { error } = await supabaseClient.from('job_ledger').update({ status: 'Cancelled' }).eq('id', jobId);
    if (error) { alert('Cancel failed: ' + error.message); return; }

    showToastNotification(`Job #100${job.quote_number} cancelled`);
    await syncStateWithDatabaseCluster();
    document.getElementById('drilldown-action-task-viewport').innerHTML = `
        <div class="text-center py-24 text-slate-500 text-xs">
            Job cancelled. Select another job to inspect.
        </div>
    `;
}

// Clone an existing job as a new Lead
async function cloneJobAsNewLead(jobId) {
    const source = liveJobsPipeline.find(j => j.id === jobId);
    if (!source) return;

    if (!confirm(`Clone job #100${source.quote_number} — ${source.customer_name}?\n\nA new Lead will be created with the same customer details and items. Dates, deposit and status will be reset.`)) return;

    // Get the next quote number
    const maxQuoteNum = liveJobsPipeline.reduce((max, j) => Math.max(max, parseInt(j.quote_number || 0)), 0);
    const newQuoteNum = maxQuoteNum + 1;

    const clonePayload = {
        quote_number:         newQuoteNum,
        customer_name:        source.customer_name,
        customer_contact:     source.customer_contact || '',
        installation_address: source.installation_address,
        job_items:            source.job_items,
        gross_revenue:        source.gross_revenue,
        deposit_amount:       source.deposit_amount || 150,
        status:               'Lead',
        deposit_paid:         false,
        install_date:         null,
        prep_date:            null,
        notes:                source.notes ? `[Cloned from #100${source.quote_number}]\n${source.notes}` : `[Cloned from #100${source.quote_number}]`,
        notes_updated_at:     null,
        sketchup_url:         source.sketchup_url || null
    };

    const { data, error } = await supabaseClient.from('job_ledger').insert(clonePayload).select().single();

    if (error) {
        alert('Clone failed: ' + error.message);
        return;
    }

    showToastNotification(`Cloned as #100${newQuoteNum} ✓`);
    // Realtime will trigger re-render, but open the new job immediately
    await syncStateWithDatabaseCluster();
    openActiveJobDrilldownActionView(data.id);
}

// Click-to-Set Stepper State Command Router
async function mutateJobPipelineNodeStateDirect(jobId, targetNodeString) {
    const { error } = await supabaseClient.from('job_ledger').update({ status: targetNodeString }).eq('id', jobId);
    if (error) {
        alert(error.message);
    } else {
        // Auto-log balance payment when job moves to Settled
        if (targetNodeString === 'Settled') {
            const job = liveJobsPipeline.find(j => j.id === jobId);
            if (job) {
                const gross      = parseFloat(job.gross_revenue || 0);
                const depositAmt = parseFloat(job.deposit_amount || 150);
                const balance    = Math.max(0, gross - depositAmt);
                if (balance > 0) {
                    await addAutoTransaction(
                        `Balance payment — ${job.customer_name} (#100${job.quote_number})`,
                        balance, 'income', 'Job Income',
                        `#100${job.quote_number}`
                    );
                }
            }
        }
        await syncStateWithDatabaseCluster();
        openActiveJobDrilldownActionView(jobId);
    }
}

// Compile Unique Calendar Prep Mondays Set List
function buildWeeklyBatchFilterDropdownSlices() {
    const dropdown = document.getElementById('batch-week-filter-dropdown');
    if (!dropdown) return;

    let uniqueMondaysAnchorListSet = new Set();
    liveJobsPipeline.forEach(j => { if(j.prep_date) uniqueMondaysAnchorListSet.add(j.prep_date); });
    const sortedMondaysListArray = Array.from(uniqueMondaysAnchorListSet).sort();
    
    if (sortedMondaysListArray.length === 0) {
        dropdown.innerHTML = `<option value="">No Active Prep Run Schedules Linked</option>`;
        return;
    }
    dropdown.innerHTML = sortedMondaysListArray.map(m => `<option value="${m}">Week commencing Monday: ${m}</option>`).join('');
    runWeeklyBatchConsolidationCalculations();
}

// PERSISTENT HIGHLIGHT SAVING ENGINE: Mutates and serializes the array down to Supabase live rows
async function commitPersistentCutBlockToggleProgress(jobId, uniquePartCompositeHashString) {
    const targetJobInstance = liveJobsPipeline.find(j => j.id === jobId);
    if (!targetJobInstance) return;

    let existingCutsListArray = [];
    try {
        existingCutsListArray = typeof targetJobInstance.obstructions === 'string' 
            ? JSON.parse(targetJobInstance.obstructions) 
            : targetJobInstance.obstructions || [];
    } catch (err) {
        existingCutsListArray = [];
    }

    if (!Array.isArray(existingCutsListArray)) existingCutsListArray = [];

    // If already checked off, pull it out to reset. Otherwise, push it in.
    if (existingCutsListArray.includes(uniquePartCompositeHashString)) {
        existingCutsListArray = existingCutsListArray.filter(item => item !== uniquePartCompositeHashString);
    } else {
        existingCutsListArray.push(uniquePartCompositeHashString);
    }

    // Direct data transport write
    const { error } = await supabaseClient
        .from('job_ledger')
        .update({ obstructions: existingCutsListArray })
        .eq('id', jobId);

    if (error) {
        console.error("Ledger sync trace error: ", error.message);
    } else {
        // Soft refresh local memory tracking arrays cleanly without causing a full layout repaint jitter
        targetJobInstance.obstructions = existingCutsListArray;
    }
}

// Toggle graphical state styles on the layout viewport block item
function toggleGraphicalCutBlockState(blockElement, jobId, uniquePartCompositeHashString) {
    if (blockElement.classList.contains('bg-brand-500/10')) {
        blockElement.classList.remove('bg-brand-500/10', 'border-brand-500/30');
        blockElement.classList.add('bg-emerald-500', 'border-emerald-600', 'text-slate-950');
        blockElement.querySelector('.cut-length-text').classList.replace('text-white', 'text-slate-950');
        blockElement.querySelector('.cut-code-text').classList.replace('text-slate-400', 'text-slate-900');
    } else {
        blockElement.classList.remove('bg-emerald-500', 'border-emerald-600', 'text-slate-950');
        blockElement.classList.add('bg-brand-500/10', 'border-brand-500/30');
        blockElement.querySelector('.cut-length-text').classList.replace('text-slate-950', 'text-white');
        blockElement.querySelector('.cut-code-text').classList.replace('text-slate-900', 'text-slate-400');
    }

    // Call state saver to lock progress inside cloud table instances
    commitPersistentCutBlockToggleProgress(jobId, uniquePartCompositeHashString);
}

// Consolidate and Nest Weekly Slicing Profiles Using High-Legibility Graphical 1D Bars with Persistence
function runWeeklyBatchConsolidationCalculations() {
    const dropdown = document.getElementById('batch-week-filter-dropdown');
    if (!dropdown || !dropdown.value) return;

    const chosenMondayAnchorDateISOKeyString = dropdown.value;
    const weeklyJobsSubCollectionList = liveJobsPipeline.filter(j => j.prep_date === chosenMondayAnchorDateISOKeyString && j.status !== 'Settled' && j.status !== 'Completed' && j.status !== 'Cancelled');

    const emerysBox = document.getElementById('batch-emerys-list');
    const toolstationBox = document.getElementById('batch-toolstation-list');
    const bfdDumpBox = document.getElementById('batch-bfd-timber-packing-dump');
    const sheetDumpBox = document.getElementById('batch-sheet-cut-dump');

    if (weeklyJobsSubCollectionList.length === 0) {
        emerysBox.innerHTML = `<li class="text-slate-500 italic">No active batch components staged for this week.</li>`;
        toolstationBox.innerHTML = `<li class="text-slate-500 italic">No active batch components staged for this week.</li>`;
        bfdDumpBox.innerHTML = `<p class="text-slate-500 text-sm italic text-center py-12 bg-slate-950 rounded-xl border border-slate-900/40 w-full">Clear batch grid window matrix blocks parameters empty.</p>`;
        sheetDumpBox.innerHTML = `<p class="text-slate-500 text-sm italic text-center py-12 bg-slate-950 rounded-xl border border-slate-900/40 w-full">Clear batch grid window matrix blocks parameters empty.</p>`;
        return;
    }

    let combinedTimberSumLengths = 0;
    let combinedSheetsSumArea = 0;
    let totalWoodScrewsCountAccumulator = 0;
    let masterBatchRawTimberCuttingLengthsList = [];

    weeklyJobsSubCollectionList.forEach(job => {
        let productItemsArray = [];
        try { productItemsArray = typeof job.job_items === 'string' ? JSON.parse(job.job_items) : job.job_items || []; } catch(e) { productItemsArray = []; }

        productItemsArray.forEach(item => {
            const H = item.height_mm; const W = item.width_mm; const D = item.depth_mm; const S = item.shelves_count;
            const internalNogginLength = D - 76;

            for(let i=0; i<4; i++) masterBatchRawTimberCuttingLengthsList.push({ len: H, code: `J100${job.quote_number}-UR`, id: job.id, key: `UR-${H}-${i}` });
            for(let i=0; i<(S*2); i++) masterBatchRawTimberCuttingLengthsList.push({ len: W, code: `J100${job.quote_number}-MR`, id: job.id, key: `MR-${W}-${i}` });
            
            const nogginsPerShelf = Math.max(1, Math.round(W / 610));
            const totalNogginsQty = S * nogginsPerShelf;
            for(let i=0; i<totalNogginsQty; i++) masterBatchRawTimberCuttingLengthsList.push({ len: internalNogginLength, code: `J100${job.quote_number}-NG`, id: job.id, key: `NG-${internalNogginLength}-${i}` });

            if (item.name.includes("Master")) { combinedTimberSumLengths += 13; combinedSheetsSumArea += 3; totalWoodScrewsCountAccumulator += 90; }
            else if (item.name.includes("Workshop")) { combinedTimberSumLengths += 24; combinedSheetsSumArea += 6; totalWoodScrewsCountAccumulator += 180; }
            else { combinedTimberSumLengths += 6; combinedSheetsSumArea += 1.5; totalWoodScrewsCountAccumulator += 40; }
        });
    });

    const stockLengthLimit = 2400;
    let sortedCutRequirements = [...masterBatchRawTimberCuttingLengthsList].sort((a,b) => b.len - a.len);
    let packedTimberBinUnits = [];

    sortedCutRequirements.forEach(part => {
        if(part.len > stockLengthLimit) return;
        let bestFitBinMatchIndex = -1; let minRemainingSpaceFound = stockLengthLimit + 1;
        for (let bIdx = 0; bIdx < packedTimberBinUnits.length; bIdx++) {
            if (packedTimberBinUnits[bIdx].remaining >= part.len) {
                let spaceAfterCutCheckVal = packedTimberBinUnits[bIdx].remaining - part.len;
                if (spaceAfterCutCheckVal < minRemainingSpaceFound) { minRemainingSpaceFound = spaceAfterCutCheckVal; bestFitBinMatchIndex = bIdx; }
            }
        }
        if (bestFitBinMatchIndex >= 0) { packedTimberBinUnits[bestFitBinMatchIndex].items.push(part); packedTimberBinUnits[bestFitBinMatchIndex].remaining -= part.len; }
        else { packedTimberBinUnits.push({ remaining: stockLengthLimit - part.len, items: [part] }); }
    });

    const finalTimberUnitsRounded = Math.ceil(combinedTimberSumLengths);
    const finalSheetsUnitsRounded = Math.ceil(combinedSheetsSumArea);

    emerysBox.innerHTML = `
        <li class="flex items-center gap-3"><input type="checkbox" class="accent-brand-500 w-5 h-5 rounded cursor-pointer"><span><strong>${packedTimberBinUnits.length}x</strong> Full Length Planks (C16 CLS 38x63mm @ 2.4m Stock)</span></li>
        <li class="flex items-center gap-3"><input type="checkbox" class="accent-brand-500 w-5 h-5 rounded cursor-pointer"><span><strong>${finalSheetsUnitsRounded}x</strong> Full Sheets 18mm OSB3 Trade Slabs (2440x1220mm)</span></li>
    `;

    toolstationBox.innerHTML = `
        <li class="flex items-center gap-3"><input type="checkbox" class="accent-emerald-500 w-5 h-5 rounded cursor-pointer"><span><strong>${Math.ceil(totalWoodScrewsCountAccumulator / 200)}x</strong> Boxes 4.0x40mm Wood Fasteners (200 Qty Pack)</span></li>
        <li class="flex items-center gap-3"><input type="checkbox" class="accent-emerald-500 w-5 h-5 rounded cursor-pointer"><span><strong>${Math.ceil((weeklyJobsSubCollectionList.length * 6) / 100) || 1}x</strong> Box 7.5x100mm Masonry Torx Anchors</span></li>
    `;

    // Render visual cut elements with durable persistence lookups mapped over row lists
    bfdDumpBox.innerHTML = packedTimberBinUnits.map((bin, idx) => {
        let itemsMarkup = bin.items.map(part => {
            const widthPercentage = (part.len / stockLengthLimit) * 100;
            
            // Resolve this job instance's historical checked logs arrays safely
            const linkedJobRow = weeklyJobsSubCollectionList.find(j => j.id === part.id);
            let savedCutsArray = [];
            try { savedCutsArray = typeof linkedJobRow.obstructions === 'string' ? JSON.parse(linkedJobRow.obstructions) : linkedJobRow.obstructions || []; } catch(e) { savedCutsArray = []; }
            if(!Array.isArray(savedCutsArray)) savedCutsArray = [];

            // Compile target composite trace key parameters
            const traceProgressKey = `${part.code}-${part.key}`;
            const isCutAlreadyCompletedInDatabase = savedCutsArray.includes(traceProgressKey);

            // Determine rendering style definitions matching state variables
            const blockStyleClasses = isCutAlreadyCompletedInDatabase 
                ? "bg-emerald-500 border-emerald-600 text-slate-950" 
                : "bg-brand-500/10 border-brand-500/30 text-white";
            const primaryTextClasses = isCutAlreadyCompletedInDatabase ? "text-slate-950" : "text-white";
            const secondaryCodeClasses = isCutAlreadyCompletedInDatabase ? "text-slate-900" : "text-slate-400";

            return `
                <div onclick="toggleGraphicalCutBlockState(this, '${part.id}', '${traceProgressKey}')" class="h-14 border-r-2 border-slate-950 flex flex-col justify-center items-center cursor-pointer select-none group relative border transition-all ${blockStyleClasses}" style="width: ${widthPercentage}%;">
                    <span class="cut-length-text font-black text-xs sm:text-sm tracking-tight ${primaryTextClasses}">${part.len}mm</span>
                    <span class="cut-code-text text-[11px] font-bold tracking-tight truncate w-full text-center px-1 ${secondaryCodeClasses}">${part.code}</span>
                    <div class="absolute bottom-full left-1/2 transform -translate-x-1/2 mb-2 bg-slate-950 border border-slate-700 text-white text-xs p-2 rounded-lg shadow-xl pointer-events-none opacity-0 group-hover:opacity-100 transition-opacity z-10 whitespace-nowrap">
                        <strong>Part:</strong> ${part.code}<br/><strong>Drop Target:</strong> ${part.len}mm<br/><span class="text-brand-400 font-bold">Click to toggle cutting log</span>
                    </div>
                </div>
            `;
        }).join('');

        const wastePercentage = (bin.remaining / stockLengthLimit) * 100;
        if (bin.remaining > 0) {
            itemsMarkup += `
                <div class="h-14 bg-slate-800/80 flex flex-col justify-center items-center text-xs font-bold text-slate-500 italic" style="width: ${wastePercentage}%;">
                    <span>${bin.remaining}mm</span>
                </div>
            `;
        }

        return `
            <div class="bg-slate-950 p-5 rounded-xl border border-slate-800/80 space-y-3 shadow-md">
                <div class="flex justify-between items-center text-sm border-b border-slate-900 pb-2">
                    <span class="text-slate-200 font-black tracking-wide">STOCK TIMBER UNIT #0${idx + 1}</span>
                    <span class="text-xs bg-slate-900 px-3 py-1 rounded-lg border border-slate-800 text-slate-400 font-medium">Waste Margin: <strong class="text-brand-400 font-black">${bin.remaining}mm</strong></span>
                </div>
                <div class="w-full bg-slate-900 h-14 rounded-xl overflow-hidden border border-slate-800 flex items-center shadow-inner">
                    ${itemsMarkup}
                </div>
            </div>
        `;
    }).join('');

    // Sheet panel cut list — per job, per unit breakdown
    sheetDumpBox.innerHTML = weeklyJobsSubCollectionList.map(job => {
        let productItemsArray = [];
        try { productItemsArray = typeof job.job_items === 'string' ? JSON.parse(job.job_items) : job.job_items || []; } catch(e) { productItemsArray = []; }
        if (productItemsArray.length === 0) return '';

        const cutsHTML = productItemsArray.map((item, idx) => {
            const W = item.width_mm, D = item.depth_mm, S = item.shelves_count;
            const unitLabel = item.name || `Unit ${idx + 1}`;
            const isWorkbench = (item.name || '').includes('Workbench');
            const boardQty = isWorkbench ? S + 1 : S;
            return `
                <div class="bg-slate-900 rounded-lg border border-slate-800 p-3 space-y-2">
                    <p class="text-xs font-black text-slate-300 uppercase tracking-wide truncate">${unitLabel}</p>
                    <div class="space-y-1">
                        <div class="flex justify-between items-center text-xs">
                            <span class="text-slate-400">Shelf Boards</span>
                            <span class="font-mono font-bold text-white">${W} × ${D}mm <span class="text-slate-500 font-normal">×${boardQty}</span></span>
                        </div>
                    </div>
                </div>
            `;
        }).join('');

        return `
            <div class="bg-slate-950 p-5 rounded-xl border border-slate-800/80 space-y-3 shadow-md">
                <div class="flex justify-between items-center text-sm border-b border-slate-900 pb-2">
                    <span class="text-slate-200 font-black tracking-wide">#100${job.quote_number} — ${job.customer_name}</span>
                    <span class="text-xs bg-slate-900 px-3 py-1 rounded-lg border border-slate-800 text-slate-400 font-medium">${productItemsArray.length} unit${productItemsArray.length !== 1 ? 's' : ''}</span>
                </div>
                <div class="grid grid-cols-1 gap-2">
                    ${cutsHTML}
                </div>
            </div>
        `;
    }).join('');
}

// ─── FINANCIALS ENGINE ────────────────────────────────────────────────────────

// In-memory expense store (persisted to Supabase configuration_ledger as JSON blob)
let dbExpenses = [];

// Estimate material COGS for a job based on its items using the BFD logic constants
// ─── PRODUCT RECIPE ENGINE v2 — Physics-based material calculator ─────────────

let dbRecipes = {}; // keyed by product id — stores build spec, not ingredient list

// ── Material unit prices — read from dbConfigs with fallbacks ──
function matPrices() {
    return {
        timberPerMetre: parseFloat(dbConfigs['timber_price_per_metre'] || 2.10),
        osbPerSheet:    parseFloat(dbConfigs['osb_price_per_sheet']    || 18.50),
        screwPerBox:    parseFloat(dbConfigs['screw_box_price']         || 7.50),   // 200x box
        anchorPerBox:   parseFloat(dbConfigs['anchor_box_price']        || 9.00),   // 100x box
        plugPerPack:    parseFloat(dbConfigs['wall_plug_price']          || 3.50),   // 100x pack
        battenPerMetre: parseFloat(dbConfigs['batten_price_per_metre']   || 1.80),   // 38×38 batten
    };
}

// ── Default build spec for a new SKU ──
function defaultBuildSpec(product) {
    const name = (product.name || '').toLowerCase();
    const isWorkbench = name.includes('workbench') || name.includes('workshop');
    const isLarge     = name.includes('master') || isWorkbench;
    return {
        // Physical envelope (mm)
        width_mm:           isWorkbench ? 2400 : isLarge ? 1800 : 1200,
        depth_mm:           isWorkbench ? 600  : 400,
        height_mm:          isWorkbench ? 900  : isLarge ? 2000 : 1800,
        // Shelf configuration
        default_shelves:    isWorkbench ? 1    : isLarge ? 4    : 3,
        shelf_height_mm:    isWorkbench ? 300  : null,   // workbench: work surface height from floor (null = equal spacing)
        shelf_cost_delta:   15.00,    // £ added to retail price per extra shelf tier
        // Structural rules
        max_unsupported_span_mm: 1200,  // kept for reference; uprights now based on heavy_duty flag
        anchors_per_upright:     2,     // top + bottom wall fixings per upright
        screws_per_joint:        3,     // screws per timber-to-timber joint
        // Options
        heavy_duty:         isWorkbench,  // adds shelf support battens under each shelf
        noggins:            false,        // horizontal noggins between uprights for rigidity
        // Material stock lengths (mm) — CLS timber comes in these lengths
        cls_stock_length:   2400,
        // OSB sheet dimensions
        osb_sheet_w:        2440,
        osb_sheet_d:        1220,
    };
}

// ── Core material calculator — takes a build spec, returns full cut list + costs ──
function calculateMaterialsFromSpec(spec) {
    const p = matPrices();

    const W = spec.width_mm;
    const D = spec.depth_mm;
    const H = spec.height_mm;
    const shelves     = spec.default_shelves;
    const cls         = spec.cls_stock_length || 2400;
    const osbW        = spec.osb_sheet_w || 2440;
    const osbD        = spec.osb_sheet_d || 1220;
    const anchPerUp   = spec.anchors_per_upright || 2;
    const screwPerJt  = spec.screws_per_joint    || 3;
    const heavyDuty   = spec.heavy_duty || false;
    const noggins     = spec.noggins    || false;

    // ── Uprights ──
    // Standard: 2 uprights (left + right). Heavy duty adds 1 centre upright.
    const totalUprights    = heavyDuty ? 3 : 2;
    const uprightLength_mm = H; // full height each

    // ── Ledger rails (horizontal timbers front + back) ──
    // 2 ledger rails per shelf tier (front + back). Top/bottom caps are structural, not ledgers.
    const ledgerCount       = shelves * 2;   // front + back per shelf
    const ledgerLength_mm   = W - (38 * 2); // span between uprights (38mm each side)

    // ── Shelf boards (OSB) ──
    // Each shelf board: width × depth. Calculate yield from full sheet.
    const shelvesTotal    = shelves + 2; // include top cap and floor board
    const boardsPerSheetW = Math.floor(osbW / W);
    const boardsPerSheetD = Math.floor(osbD / D);
    const boardsPerSheet  = Math.max(1, boardsPerSheetW * boardsPerSheetD);
    const osbSheets       = Math.ceil(shelvesTotal / boardsPerSheet);

    // ── Shelf support battens (heavy duty only) ──
    // One batten front-to-back under each shelf board (not top cap or floor board)
    const battenLength_mm = D;
    const battenCount     = heavyDuty ? shelves : 0; // one per intermediate shelf

    // ── Noggins (optional cross-bracing) ──
    // One noggin between each pair of uprights, at mid-height
    const nogginCount     = noggins ? (totalUprights - 1) : 0;
    const nogginLength_mm = Math.round((W / (totalUprights - 1)) - 38); // gap between uprights minus one upright width

    // ── Fixings ──
    // Anchors: anchors_per_upright × total uprights
    const anchorsTotal = totalUprights * anchPerUp;
    const anchorBoxes  = Math.ceil(anchorsTotal / 100);

    // Wall plugs match anchors
    const plugBoxes = Math.ceil(anchorsTotal / 100);

    // Screws: joints = (shelves+2) × (totalUprights-1) per rail side × 2 + upright joints
    const railJoints    = ledgerCount * 2; // each end of each ledger
    const battenJoints  = battenCount * 2;
    const nogginJoints  = nogginCount * 2;
    const totalJoints   = railJoints + battenJoints + nogginJoints;
    const screwsTotal   = totalJoints * screwPerJt;
    const screwBoxes    = Math.ceil(screwsTotal / 200);

    // ── CLS metre totals ──
    // Round each cut up to nearest stock length, tally metres
    function cuttingWaste(length_mm, qty) {
        // How many cuts from one stock length, ceiling, × qty
        const cutsPerLength = Math.floor(cls / length_mm);
        const lengthsNeeded = Math.ceil(qty / cutsPerLength);
        return (lengthsNeeded * cls) / 1000; // in metres
    }

    const timberMetres =
        cuttingWaste(uprightLength_mm, totalUprights) +
        cuttingWaste(ledgerLength_mm,  ledgerCount)   +
        (heavyDuty ? cuttingWaste(battenLength_mm, battenCount) : 0) +
        (noggins   ? cuttingWaste(nogginLength_mm,  nogginCount) : 0);

    // ── Costs ──
    const costTimber  = timberMetres * p.timberPerMetre;
    const costOSB     = osbSheets    * p.osbPerSheet;
    const costScrews  = screwBoxes   * p.screwPerBox;
    const costAnchors = anchorBoxes  * p.anchorPerBox;
    const costPlugs   = plugBoxes    * p.plugPerPack;
    const costBattens = heavyDuty ? (battenCount * (battenLength_mm / 1000) * p.battenPerMetre) : 0;
    const totalCOGS   = costTimber + costOSB + costScrews + costAnchors + costPlugs + costBattens;

    // ── Per-shelf delta cost ──
    // One extra ledger pair + one OSB board + battens if heavy duty
    const extraLedgerMetres = cuttingWaste(ledgerLength_mm, 2); // front + back rail
    const extraOSBFraction  = 1 / boardsPerSheet;
    const extraBattenMetres = heavyDuty ? (battenLength_mm / 1000) : 0;
    const shelfDeltaCost = (extraLedgerMetres * p.timberPerMetre) +
                           (extraOSBFraction  * p.osbPerSheet)    +
                           (extraBattenMetres * p.battenPerMetre);

    return {
        // Cut list
        cuts: [
            { label: 'Uprights',              length_mm: uprightLength_mm, qty: totalUprights,  metres: parseFloat((totalUprights * uprightLength_mm / 1000).toFixed(2)), material: 'CLS 38×63' },
            { label: 'Ledger Rails',           length_mm: ledgerLength_mm,  qty: ledgerCount,    metres: parseFloat((ledgerCount * ledgerLength_mm / 1000).toFixed(2)),   material: 'CLS 38×63' },
            ...(heavyDuty ? [{ label: 'Shelf Support Battens', length_mm: battenLength_mm, qty: battenCount, metres: parseFloat((battenCount * battenLength_mm / 1000).toFixed(2)), material: 'CLS 38×38' }] : []),
            ...(noggins   ? [{ label: 'Noggins',               length_mm: nogginLength_mm, qty: nogginCount, metres: parseFloat((nogginCount * nogginLength_mm / 1000).toFixed(2)),  material: 'CLS 38×63' }] : []),
            { label: `Shelf Boards (${W}×${D}mm)`, length_mm: null, qty: shelvesTotal, metres: null, material: `OSB3 18mm — ${osbSheets} sheet${osbSheets !== 1 ? 's' : ''}` },
        ],
        // Fixings summary
        fixings: [
            { label: 'Masonry Anchors', qty: anchorsTotal, boxes: anchorBoxes },
            { label: 'Wall Plugs',      qty: anchorsTotal, boxes: plugBoxes   },
            { label: 'Screws 4×40mm',   qty: screwsTotal,  boxes: screwBoxes  },
        ],
        // Totals
        timberMetres:   parseFloat(timberMetres.toFixed(2)),
        osbSheets,
        totalCOGS:      parseFloat(totalCOGS.toFixed(2)),
        shelfDeltaCost: parseFloat(shelfDeltaCost.toFixed(2)),
        // Breakdown
        lineItems: [
            { label: 'CLS Timber',         qty: `${timberMetres.toFixed(1)}m`,   unit_cost: p.timberPerMetre,  total: costTimber  },
            { label: 'OSB3 Sheets',         qty: `${osbSheets} sht`,              unit_cost: p.osbPerSheet,     total: costOSB     },
            { label: 'Screws (box)',         qty: `${screwBoxes} box`,             unit_cost: p.screwPerBox,     total: costScrews  },
            { label: 'Masonry Anchors',      qty: `${anchorBoxes} box`,            unit_cost: p.anchorPerBox,    total: costAnchors },
            { label: 'Wall Plugs',           qty: `${plugBoxes} pk`,               unit_cost: p.plugPerPack,     total: costPlugs   },
            ...(heavyDuty ? [{ label: 'Support Battens', qty: `${(battenCount * battenLength_mm / 1000).toFixed(1)}m`, unit_cost: p.battenPerMetre, total: costBattens }] : []),
        ],
    };
}

async function loadRecipesFromDB() {
    try {
        const { data } = await supabaseClient.from('configuration_ledger').select('*').eq('key','pgs_product_recipes').single();
        dbRecipes = (data && data.value) ? JSON.parse(data.value) : {};
    } catch(e) {
        dbRecipes = {};
    }
    // Seed defaults for any product with no spec yet
    dbProducts.forEach(p => {
        if (!dbRecipes[p.id] || !dbRecipes[p.id].width_mm) {
            dbRecipes[p.id] = defaultBuildSpec(p);
        }
    });
}

async function saveAllRecipes() {
    // Ensure dbRecipes is initialized as an object if it's missing
    if (typeof dbRecipes === 'undefined' || !dbRecipes) {
        dbRecipes = {};
    }

    // Flush all form inputs back into dbRecipes before saving
    dbProducts.forEach(product => {
        // FIX: If this product doesn't have a recipe slot yet, create it instead of skipping
        if (!dbRecipes[product.id]) {
            dbRecipes[product.id] = {};
        }
        
        const spec = dbRecipes[product.id];
        const fields = ['width_mm','depth_mm','height_mm','default_shelves','anchors_per_upright',
                        'screws_per_joint','cls_stock_length','shelf_cost_delta','shelf_height_mm'];

        fields.forEach(f => {
            const el = document.getElementById(`rspec-${product.id}-${f}`);
            if (el) {
                const v = parseFloat(el.value);
                spec[f] = isNaN(v) ? null : v;
            }
        });
        
        const hdEl  = document.getElementById(`rspec-${product.id}-heavy_duty`);
        const nogEl = document.getElementById(`rspec-${product.id}-noggins`);
        if (hdEl)  spec.heavy_duty = hdEl.checked;
        if (nogEl) spec.noggins    = nogEl.checked;
        
        // Ensure shelf_cost_delta has a baseline value
        spec.shelf_cost_delta = spec.shelf_cost_delta || 15;
    });

    try {
        await upsertConfig('pgs_product_recipes', JSON.stringify(dbRecipes));

        // Force calculations to refresh across the planner using the newly saved rules
        if (typeof executeCoreAnalyticalMathEngineRuns === "function") {
            executeCoreAnalyticalMathEngineRuns();
        }

        showToastNotification('Recipes saved ✓');
        
        if (typeof renderRecipeEditor === "function") {
            renderRecipeEditor();
        }
    } catch (error) {
        console.error("Database save exception:", error);
        showToastNotification('Save failed ❌');
    }
}

async function createNewProduct() {
    const name    = document.getElementById('new-product-name')?.value?.trim();
    const price   = parseFloat(document.getElementById('new-product-price')?.value);
    const width   = parseInt(document.getElementById('new-product-width')?.value);
    const depth   = parseInt(document.getElementById('new-product-depth')?.value);
    const height  = parseInt(document.getElementById('new-product-height')?.value);
    const shelves = parseInt(document.getElementById('new-product-shelves')?.value) || 3;

    if (!name)            return showToastNotification('Enter a product name ❌');
    if (isNaN(price))     return showToastNotification('Enter a retail price ❌');
    if (isNaN(width) || isNaN(depth) || isNaN(height)) return showToastNotification('Enter all dimensions ❌');

    try {
        const { error } = await supabaseClient.from('product_templates').insert({
            name,
            base_retail_price: price,
            width_mm:    width,
            depth_mm:    depth,
            height_mm:   height,
            shelves_count: shelves,
        });
        if (error) throw error;

        // Clear form
        ['new-product-name','new-product-price','new-product-width','new-product-depth','new-product-height','new-product-shelves']
            .forEach(id => { const el = document.getElementById(id); if (el) el.value = ''; });

        showToastNotification(`${name} created ✓`);
        await syncStateWithDatabaseCluster();
    } catch (err) {
        console.error('Create product error:', err);
        showToastNotification('Failed to create product ❌');
    }
}

function recipePreviewLive(productId) {
    // Read current form values into a temporary spec for live preview
    const base = JSON.parse(JSON.stringify(dbRecipes[productId] || {}));
    const fields = ['width_mm','depth_mm','height_mm','default_shelves','anchors_per_upright',
                    'screws_per_joint','cls_stock_length','shelf_cost_delta','shelf_height_mm'];
    fields.forEach(f => {
        const el = document.getElementById(`rspec-${productId}-${f}`);
        if (el) {
            const v = parseFloat(el.value);
            base[f] = isNaN(v) ? null : v;
        }
    });
    const hdEl  = document.getElementById(`rspec-${productId}-heavy_duty`);
    const nogEl = document.getElementById(`rspec-${productId}-noggins`);
    if (hdEl)  base.heavy_duty = hdEl.checked;
    if (nogEl) base.noggins    = nogEl.checked;

    const result = calculateMaterialsFromSpec(base);
    const product = dbProducts.find(p => p.id === productId);
    const price   = parseFloat(product?.base_retail_price || 0);
    const margin  = price > 0 ? ((price - result.totalCOGS) / price * 100) : 0;
    const mCol    = margin >= 60 ? '#34d399' : margin >= 40 ? '#fbbf24' : '#f87171';

    // Update live preview panel
    const preview = document.getElementById(`recipe-preview-${productId}`);
    if (!preview) return;

    const cutRows = result.cuts.map(c => `
        <tr class="border-b border-slate-800/60">
            <td class="py-1.5 text-slate-300 font-medium">${c.label}</td>
            <td class="py-1.5 text-center text-slate-400">${c.material}</td>
            <td class="py-1.5 text-center text-brand-400 font-bold">${c.length_mm ? c.length_mm + 'mm' : '—'}</td>
            <td class="py-1.5 text-center text-white font-black">${c.qty}</td>
            <td class="py-1.5 text-right text-slate-400">${c.metres ? c.metres + 'm' : c.material.split('—')[1]?.trim() || '—'}</td>
        </tr>
    `).join('');

    const fixingRows = result.fixings.map(f => `
        <tr class="border-b border-slate-800/40">
            <td class="py-1 text-slate-300">${f.label}</td>
            <td class="py-1 text-center text-slate-400">${f.qty}</td>
            <td class="py-1 text-right text-white font-bold">${f.boxes} box${f.boxes !== 1 ? 'es' : ''}</td>
        </tr>
    `).join('');

    const lineRows = result.lineItems.map(l => `
        <tr class="border-b border-slate-800/40">
            <td class="py-1 text-slate-300">${l.label}</td>
            <td class="py-1 text-center text-slate-400">${l.qty}</td>
            <td class="py-1 text-right text-white font-bold">£${l.total.toFixed(2)}</td>
        </tr>
    `).join('');

    preview.innerHTML = `
        <div class="grid grid-cols-1 md:grid-cols-3 gap-4 mb-4">
            <div class="bg-slate-900 rounded-xl p-3 text-center border border-slate-800">
                <p class="text-[10px] uppercase font-bold text-slate-500 mb-1">Material COGS</p>
                <p class="text-rose-400 font-black text-xl">£${result.totalCOGS.toFixed(2)}</p>
            </div>
            <div class="bg-slate-900 rounded-xl p-3 text-center border border-slate-800">
                <p class="text-[10px] uppercase font-bold text-slate-500 mb-1">Gross Profit</p>
                <p class="text-emerald-400 font-black text-xl">£${(price - result.totalCOGS).toFixed(2)}</p>
            </div>
            <div class="bg-slate-900 rounded-xl p-3 text-center border border-slate-800">
                <p class="text-[10px] uppercase font-bold text-slate-500 mb-1">Margin</p>
                <p class="font-black text-xl" style="color:${mCol}">${margin.toFixed(0)}%</p>
            </div>
        </div>

        <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
            <!-- Cut list -->
            <div class="bg-slate-900 rounded-xl border border-slate-800 overflow-hidden">
                <div class="px-3 py-2 border-b border-slate-800 text-[10px] font-black uppercase text-slate-400 tracking-wider">Cut List</div>
                <table class="w-full text-xs px-3">
                    <thead><tr class="text-[10px] text-slate-600 uppercase border-b border-slate-800">
                        <th class="text-left py-1.5 px-3">Item</th><th class="text-center py-1.5">Material</th>
                        <th class="text-center py-1.5">Length</th><th class="text-center py-1.5">Qty</th>
                        <th class="text-right py-1.5 pr-3">Total</th>
                    </tr></thead>
                    <tbody class="px-3">${cutRows}</tbody>
                </table>
                <div class="px-3 py-2 border-t border-slate-800 text-[10px] text-slate-500">
                    <strong class="text-brand-400">${result.timberMetres}m</strong> CLS total · <strong class="text-brand-400">${result.osbSheets}</strong> OSB sheet${result.osbSheets !== 1 ? 's' : ''}
                </div>
            </div>

            <!-- Cost breakdown -->
            <div class="bg-slate-900 rounded-xl border border-slate-800 overflow-hidden">
                <div class="px-3 py-2 border-b border-slate-800 text-[10px] font-black uppercase text-slate-400 tracking-wider">Cost Breakdown</div>
                <table class="w-full text-xs">
                    <tbody class="px-3">${lineRows}</tbody>
                    <tfoot>
                        <tr class="border-t border-slate-700">
                            <td colspan="2" class="py-2 px-3 text-slate-400 font-bold uppercase text-[10px]">Total COGS</td>
                            <td class="py-2 pr-3 text-right text-rose-400 font-black">£${result.totalCOGS.toFixed(2)}</td>
                        </tr>
                    </tfoot>
                </table>
                <div class="px-3 py-2 border-t border-slate-800 text-[10px] text-slate-500">
                    Extra shelf tier adds approx <strong class="text-sky-400">£${result.shelfDeltaCost.toFixed(2)}</strong> materials cost
                </div>

                <!-- Fixings -->
                <div class="px-3 py-2 border-t border-slate-800 text-[10px] font-black uppercase text-slate-400 tracking-wider">Fixings</div>
                <table class="w-full text-xs">
                    <tbody>${fixingRows}</tbody>
                </table>
            </div>
        </div>
    `;
}

function switchRecipeTab(productId) {
    dbProducts.forEach(p => {
        const card = document.getElementById(`recipe-card-${p.id}`);
        const tab  = document.getElementById(`recipe-tab-${p.id}`);
        if (!card || !tab) return;
        if (p.id === productId) {
            card.classList.remove('hidden');
            tab.classList.add('bg-brand-500', 'text-slate-950', 'font-black');
            tab.classList.remove('text-slate-400', 'hover:text-white', 'font-bold');
            recipePreviewLive(productId);
        } else {
            card.classList.add('hidden');
            tab.classList.remove('bg-brand-500', 'text-slate-950', 'font-black');
            tab.classList.add('text-slate-400', 'hover:text-white', 'font-bold');
        }
    });
}

function renderRecipeEditor() {
    const container = document.getElementById('recipe-editor-container');
    if (!container) return;

    if (dbProducts.length === 0) {
        container.innerHTML = `<p class="text-slate-500 text-xs italic text-center py-6">No SKUs found.</p>`;
        return;
    }

    const tabsHTML = dbProducts.map((product, idx) => `
        <button id="recipe-tab-${product.id}" onclick="switchRecipeTab('${product.id}')"
            class="px-4 py-2 rounded-lg text-xs uppercase tracking-wide transition-all whitespace-nowrap ${idx === 0 ? 'bg-brand-500 text-slate-950 font-black' : 'text-slate-400 hover:text-white font-bold'}">
            ${product.name}
        </button>
    `).join('');

    const cardsHTML = dbProducts.map((product, idx) => {
        const spec  = dbRecipes[product.id] || defaultBuildSpec(product);
        const price = parseFloat(product.base_retail_price || 0);

        function numField(field, label, step = 1, hint = '') {
            return `
                <div>
                    <label class="block text-[10px] text-slate-500 uppercase font-bold mb-1">${label}${hint ? `<span class="text-slate-700 ml-1 normal-case">${hint}</span>` : ''}</label>
                    <input type="number" id="rspec-${product.id}-${field}"
                        value="${spec[field] ?? 0}" step="${step}" min="0"
                        oninput="recipePreviewLive('${product.id}')"
                        class="w-full bg-slate-900 border border-slate-700 rounded-xl p-2.5 text-white text-xs outline-none focus:border-brand-500 text-right">
                </div>`;
        }

        function toggle(field, label, hint = '') {
            return `
                <div class="flex items-start gap-3 bg-slate-900 border border-slate-700 rounded-xl p-3">
                    <input type="checkbox" id="rspec-${product.id}-${field}"
                        ${spec[field] ? 'checked' : ''}
                        onchange="recipePreviewLive('${product.id}')"
                        class="mt-0.5 accent-brand-500 w-4 h-4 cursor-pointer">
                    <div>
                        <label for="rspec-${product.id}-${field}" class="text-xs font-bold text-white cursor-pointer">${label}</label>
                        ${hint ? `<p class="text-[10px] text-slate-500 mt-0.5">${hint}</p>` : ''}
                    </div>
                </div>`;
        }

        return `
            <div id="recipe-card-${product.id}" class="${idx === 0 ? '' : 'hidden'} bg-slate-950 rounded-2xl border border-slate-800 overflow-hidden">

                <!-- SKU header bar -->
                <div class="flex justify-between items-center px-5 py-4 bg-slate-900 border-b border-slate-800">
                    <div>
                        <span class="font-black text-white">${product.name}</span>
                        <span class="ml-2 text-xs text-slate-500">Retail: <strong class="text-white">£${price.toFixed(2)}</strong></span>
                    </div>
                    <button onclick="saveAllRecipes()" class="bg-emerald-500 text-slate-950 font-black px-4 py-1.5 rounded-xl text-xs uppercase tracking-wider hover:bg-emerald-400 transition-all">Save</button>
                </div>

                <div class="p-5 space-y-6">

                    <!-- Physical envelope -->
                    <div>
                        <h4 class="text-[10px] font-black uppercase text-slate-400 tracking-wider mb-3">Physical Dimensions</h4>
                        <div class="grid grid-cols-3 gap-3">
                            ${numField('width_mm',  'Width (mm)',  10)}
                            ${numField('depth_mm',  'Depth (mm)',  10)}
                            ${numField('height_mm', 'Height (mm)', 10)}
                        </div>
                    </div>

                    <!-- Shelf configuration -->
                    <div>
                        <h4 class="text-[10px] font-black uppercase text-slate-400 tracking-wider mb-3">Shelf Configuration</h4>
                        <div class="grid grid-cols-2 gap-3">
                            ${numField('default_shelves',   'Default Shelf Tiers', 1, '(intermediate boards)')}
                            ${numField('shelf_cost_delta',  'Retail Price Uplift Per Extra Tier (£)', 0.50)}
                            ${spec.shelf_height_mm != null ? numField('shelf_height_mm', 'Work Surface Height (mm)', 10, '→ position from floor') : ''}
                        </div>
                    </div>

                    <!-- Structural rules -->
                    <div>
                        <h4 class="text-[10px] font-black uppercase text-slate-400 tracking-wider mb-3">Structural Rules</h4>
                        <div class="grid grid-cols-2 gap-3">
                            ${numField('cls_stock_length',    'CLS Stock Length (mm)',  100, '→ used for waste calc')}
                            ${numField('anchors_per_upright', 'Wall Anchors Per Upright', 1)}
                            ${numField('screws_per_joint',    'Screws Per Joint',          1)}
                        </div>
                    </div>

                    <!-- Build options -->
                    <div>
                        <h4 class="text-[10px] font-black uppercase text-slate-400 tracking-wider mb-3">Build Options</h4>
                        <div class="grid grid-cols-1 md:grid-cols-2 gap-3">
                            ${toggle('heavy_duty', 'Heavy Duty Mode', 'Adds 38×38 support battens front-to-back under every shelf for increased load capacity')}
                            ${toggle('noggins',    'Noggins',         'Horizontal CLS noggins at mid-height between uprights for racking resistance')}
                        </div>
                    </div>

                    <!-- Live preview panel -->
                    <div>
                        <h4 class="text-[10px] font-black uppercase text-slate-400 tracking-wider mb-3">Live Material Calculation</h4>
                        <div id="recipe-preview-${product.id}" class="text-xs"></div>
                    </div>

                </div>
            </div>
        `;
    }).join('');

    container.innerHTML = `
        <div class="flex gap-2 flex-wrap mb-4 p-1.5 bg-slate-900 rounded-xl border border-slate-800 overflow-x-auto">
            ${tabsHTML}
        </div>
        ${cardsHTML}
    `;

    // Trigger live preview only for the visible (first) product initially
    if (dbProducts.length > 0) recipePreviewLive(dbProducts[0].id);
}

// estimateJobCOGS — uses physics-based recipe calculation
function estimateJobCOGS(job) {
    let totalCOGS = 0;
    let items = [];
    try { items = typeof job.job_items === 'string' ? JSON.parse(job.job_items) : job.job_items || []; } catch(e) { items = []; }

    items.forEach(item => {
        const spec = dbRecipes[item.id];
        if (spec && spec.width_mm) {
            // Use a spec adjusted for the actual shelf count on this job item
            const adjustedSpec = { ...spec, default_shelves: item.shelves_count || spec.default_shelves };
            const result = calculateMaterialsFromSpec(adjustedSpec);
            totalCOGS += result.totalCOGS;
        } else {
            // Fallback estimates
            const p    = matPrices();
            const name = (item.name || '').toLowerCase();
            if (name.includes('workshop') || name.includes('workbench')) {
                totalCOGS += (24 * p.timberPerMetre) + (6 * p.osbPerSheet) + p.screwPerBox * 2 + p.anchorPerBox;
            } else if (name.includes('master') || name.includes('standard')) {
                totalCOGS += (13 * p.timberPerMetre) + (3 * p.osbPerSheet) + p.screwPerBox + p.anchorPerBox;
            } else {
                totalCOGS += (6  * p.timberPerMetre) + (1.5 * p.osbPerSheet) + p.screwPerBox + p.anchorPerBox;
            }
        }
    });
    return totalCOGS;
}

// ─── END PRODUCT RECIPE ENGINE v2 ─────────────────────────────────────────────

// Get current UK tax year (Apr 6 – Apr 5)
function getCurrentTaxYear() {
    const now = new Date();
    const y = now.getFullYear();
    const taxYearStart = new Date(y, 3, 6); // April 6
    return now >= taxYearStart ? { start: new Date(y, 3, 6), end: new Date(y + 1, 3, 5), label: `${y}/${String(y+1).slice(2)}` }
                               : { start: new Date(y-1, 3, 6), end: new Date(y, 3, 5), label: `${y-1}/${String(y).slice(2)}` };
}

// Load expenses from Supabase config ledger
async function loadExpensesFromDB() {
    try {
        const { data } = await supabaseClient.from('configuration_ledger').select('*').eq('key', 'pgs_expense_log').single();
        if (data && data.value) {
            dbExpenses = JSON.parse(data.value);
        } else {
            dbExpenses = [];
        }
    } catch(e) {
        dbExpenses = [];
    }
}

// Save expenses back to Supabase
async function saveExpensesToDB() {
    await upsertConfig('pgs_expense_log', JSON.stringify(dbExpenses));
}

// Log a new expense entry from the form
async function logNewExpenseEntry() {
    const desc   = document.getElementById('exp-description').value.trim();
    const amount = parseFloat(document.getElementById('exp-amount').value);
    const cat    = document.getElementById('exp-category').value;

    if (!desc) return alert("Please enter a description.");
    if (isNaN(amount) || amount <= 0) return alert("Please enter a valid amount.");

    const entry = {
        id: crypto.randomUUID(),
        date: new Date().toISOString().split('T')[0],
        description: desc,
        amount: amount,
        category: cat
    };

    dbExpenses.push(entry);
    await saveExpensesToDB();

    // Mirror into transaction ledger, linking back via _expense_id to prevent backfill duplication
    const txn = buildTransaction(desc, amount, 'expense', cat, entry.date, 'auto-expense', null);
    txn._expense_id = entry.id;
    dbTransactions.push(txn);
    await saveTransactionsToDB();
    renderTransactionLedger();

    document.getElementById('exp-description').value = '';
    document.getElementById('exp-amount').value = '';

    calculateGlobalTurnoverLedgerComplianceSplits();
    showToastNotification(`Expense logged: £${amount.toFixed(2)} — ${desc}`);
}

// Delete an expense entry
async function deleteExpenseEntry(id) {
    dbExpenses = dbExpenses.filter(e => e.id !== id);
    await saveExpensesToDB();
    calculateGlobalTurnoverLedgerComplianceSplits();
}

// Draw the monthly revenue bar chart using canvas
function renderMonthlyRevenueChart(monthlyTotals, year) {
    const canvas = document.getElementById('fin-monthly-chart');
    const labelRow = document.getElementById('fin-chart-month-labels');
    const yearLabel = document.getElementById('fin-chart-year-label');
    if (!canvas) return;

    // Store data on the canvas element so we can redraw on tab switch
    canvas._chartData = { monthlyTotals, year };

    const monthNames = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
    if (yearLabel) yearLabel.textContent = `Tax Year ${year}`;
    if (labelRow) labelRow.innerHTML = monthNames.map(m => `<span>${m}</span>`).join('');

    const ctx = canvas.getContext('2d');
    const dpr = window.devicePixelRatio || 1;
    const rect = canvas.parentElement.getBoundingClientRect();

    // Fall back to explicit dimensions when the tab is hidden (rect returns 0x0)
    const W = rect.width  || canvas.parentElement.offsetWidth  || 600;
    const H = rect.height || canvas.parentElement.offsetHeight || 192;

    canvas.width  = W * dpr;
    canvas.height = H * dpr;
    canvas.style.width  = W + 'px';
    canvas.style.height = H + 'px';
    ctx.scale(dpr, dpr);
    const maxVal = Math.max(...monthlyTotals, 1);
    const barCount = 12;
    const sidePad = 10;
    const barW = (W - sidePad * 2) / barCount;
    const barPad = barW * 0.2;
    const topPad = 20;
    const bottomPad = 8;
    const chartH = H - topPad - bottomPad;

    ctx.clearRect(0, 0, W, H);

    const today = new Date();

    monthlyTotals.forEach((val, i) => {
        const x = sidePad + i * barW + barPad / 2;
        const bW = barW - barPad;
        const bH = (val / maxVal) * chartH;
        const y = topPad + chartH - bH;

        const isCurrentMonth = (today.getMonth() === i);
        const isEmpty = val === 0;

        // Bar fill
        const grad = ctx.createLinearGradient(x, y, x, y + bH);
        if (isEmpty) {
            grad.addColorStop(0, 'rgba(30,41,59,0.5)');
            grad.addColorStop(1, 'rgba(15,23,42,0.3)');
        } else if (isCurrentMonth) {
            grad.addColorStop(0, '#ffaa44');
            grad.addColorStop(1, '#FF8700');
        } else {
            grad.addColorStop(0, 'rgba(255,135,0,0.5)');
            grad.addColorStop(1, 'rgba(255,135,0,0.2)');
        }

        ctx.fillStyle = grad;
        const radius = 4;
        ctx.beginPath();
        ctx.moveTo(x + radius, y);
        ctx.lineTo(x + bW - radius, y);
        ctx.quadraticCurveTo(x + bW, y, x + bW, y + radius);
        ctx.lineTo(x + bW, y + bH);
        ctx.lineTo(x, y + bH);
        ctx.lineTo(x, y + radius);
        ctx.quadraticCurveTo(x, y, x + radius, y);
        ctx.closePath();
        ctx.fill();

        // Value label above bar
        if (val > 0) {
            ctx.fillStyle = isCurrentMonth ? '#ffaa44' : 'rgba(255,170,68,0.7)';
            ctx.font = `bold ${Math.max(8, Math.min(10, bW * 0.35))}px Inter, sans-serif`;
            ctx.textAlign = 'center';
            ctx.fillText(`£${val >= 1000 ? (val/1000).toFixed(1)+'k' : val.toFixed(0)}`, x + bW / 2, y - 4);
        }
    });
}

// Render per-job margin breakdown list
function renderJobMarginList(settledJobs) {
    const container = document.getElementById('fin-job-margin-list');
    if (!container) return;

    if (settledJobs.length === 0) {
        container.innerHTML = `<p class="text-slate-500 text-xs italic text-center py-8">No settled jobs yet.</p>`;
        return;
    }

    container.innerHTML = [...settledJobs].reverse().map(job => {
        const revenue = parseFloat(job.gross_revenue || 0);
        const cogs    = estimateJobCOGS(job);
        const profit  = revenue - cogs;
        const margin  = revenue > 0 ? (profit / revenue * 100) : 0;
        const marginColor = margin >= 60 ? 'text-emerald-400' : margin >= 40 ? 'text-amber-400' : 'text-rose-400';
        const barWidth = Math.max(0, Math.min(100, margin));

        return `
            <div class="bg-slate-950 p-3 rounded-xl border border-slate-800 space-y-2">
                <div class="flex justify-between items-center text-xs">
                    <span class="font-black text-white">#100${job.quote_number} — ${job.customer_name}</span>
                    <span class="${marginColor} font-black">${margin.toFixed(0)}% margin</span>
                </div>
                <div class="flex justify-between text-[10px] text-slate-400">
                    <span>Revenue: <strong class="text-white">£${revenue.toFixed(2)}</strong></span>
                    <span>Est. COGS: <strong class="text-rose-400">£${cogs.toFixed(2)}</strong></span>
                    <span>Profit: <strong class="text-emerald-400">£${profit.toFixed(2)}</strong></span>
                </div>
                <div class="w-full bg-slate-900 h-1.5 rounded-full overflow-hidden">
                    <div class="h-full rounded-full transition-all duration-500 ${margin >= 60 ? 'bg-emerald-500' : margin >= 40 ? 'bg-amber-500' : 'bg-rose-500'}" style="width: ${barWidth}%"></div>
                </div>
            </div>
        `;
    }).join('');
}

// Render expense log list
function renderExpenseList() {
    const container = document.getElementById('fin-expense-list');
    if (!container) return;

    if (dbExpenses.length === 0) {
        container.innerHTML = `<p class="text-slate-500 text-xs italic text-center py-6">No expenses logged yet.</p>`;
        return;
    }

    const catColors = {
        Materials: 'text-brand-400 bg-brand-500/10 border-brand-500/20',
        Hardware:  'text-sky-400 bg-sky-500/10 border-sky-500/20',
        Tools:     'text-purple-400 bg-purple-500/10 border-purple-500/20',
        Transport: 'text-amber-400 bg-amber-500/10 border-amber-500/20',
        Marketing: 'text-pink-400 bg-pink-500/10 border-pink-500/20',
        Other:     'text-slate-400 bg-slate-800 border-slate-700'
    };

    container.innerHTML = [...dbExpenses].reverse().map(e => {
        const colorClass = catColors[e.category] || catColors.Other;
        return `
            <div class="flex items-center justify-between gap-2 bg-slate-950 p-2.5 rounded-xl border border-slate-800 text-xs">
                <div class="flex-1 min-w-0">
                    <span class="font-bold text-white block truncate">${e.description}</span>
                    <span class="text-slate-500">${e.date}</span>
                </div>
                <span class="px-2 py-0.5 rounded-lg border text-[10px] font-bold uppercase ${colorClass} whitespace-nowrap">${e.category}</span>
                <span class="font-black text-rose-400 whitespace-nowrap">£${parseFloat(e.amount).toFixed(2)}</span>
                <button onclick="deleteExpenseEntry('${e.id}')" class="text-slate-600 hover:text-rose-400 transition-all font-black text-sm leading-none px-1">×</button>
            </div>
        `;
    }).join('');
}

// ─── TRANSACTION LEDGER ENGINE ────────────────────────────────────────────────

let dbTransactions = [];       // in-memory store
let txnFilterType  = 'all';    // 'all' | 'income' | 'expense'

// Load transactions from Supabase config ledger
async function loadTransactionsFromDB() {
    try {
        const { data } = await supabaseClient.from('configuration_ledger').select('*').eq('key', 'pgs_transaction_log').single();
        dbTransactions = (data && data.value) ? JSON.parse(data.value) : [];
    } catch(e) {
        dbTransactions = [];
    }

    // Backfill: migrate any expenses from dbExpenses that aren't already in dbTransactions.
    // This handles expenses that were logged before the transaction ledger existed.
    let backfillCount = 0;
    dbExpenses.forEach(exp => {
        // Use the expense id as a stable reference key to avoid duplicates
        const alreadyExists = dbTransactions.some(t => t.source === 'auto-expense' && t._expense_id === exp.id);
        if (!alreadyExists) {
            dbTransactions.push({
                id:          crypto.randomUUID(),
                date:        exp.date || new Date().toISOString().split('T')[0],
                description: exp.description,
                amount:      parseFloat(exp.amount),
                type:        'expense',
                category:    exp.category,
                source:      'auto-expense',
                job_ref:     null,
                _expense_id: exp.id   // stable link back to the expense record
            });
            backfillCount++;
        }
    });

    if (backfillCount > 0) {
        await saveTransactionsToDB();
        console.log(`PGS: Backfilled ${backfillCount} expense(s) into transaction ledger.`);
    }
}

// Save transactions back to Supabase
async function saveTransactionsToDB() {
    await upsertConfig('pgs_transaction_log', JSON.stringify(dbTransactions));
}

// Create a transaction object
function buildTransaction(description, amount, type, category, date, source = 'manual', jobRef = null) {
    return {
        id:          crypto.randomUUID(),
        date:        date || new Date().toISOString().split('T')[0],
        description,
        amount:      parseFloat(amount),
        type,        // 'income' | 'expense'
        category,
        source,      // 'manual' | 'auto-deposit' | 'auto-balance' | 'auto-expense'
        job_ref:     jobRef
    };
}

// Add an auto-generated transaction (called from deposit flip / status change / expense log)
async function addAutoTransaction(description, amount, type, category, jobRef = null) {
    const txn = buildTransaction(description, amount, type, category, new Date().toISOString().split('T')[0], 'auto', jobRef);
    dbTransactions.push(txn);
    await saveTransactionsToDB();
    renderTransactionLedger();
}

// Log a manual transaction from the form
async function logManualTransaction() {
    const desc   = document.getElementById('txn-description').value.trim();
    const amount = parseFloat(document.getElementById('txn-amount').value);
    const type   = document.getElementById('txn-type').value;
    const cat    = document.getElementById('txn-category').value;
    const date   = document.getElementById('txn-date').value || new Date().toISOString().split('T')[0];

    if (!desc)                        return alert("Please enter a description.");
    if (isNaN(amount) || amount <= 0) return alert("Please enter a valid amount.");

    const txn = buildTransaction(desc, amount, type, cat, date, 'manual', null);
    dbTransactions.push(txn);
    await saveTransactionsToDB();
    renderTransactionLedger();
    showToastNotification(`Transaction logged: ${type === 'income' ? '+' : '-'}£${amount.toFixed(2)}`);

    document.getElementById('txn-description').value = '';
    document.getElementById('txn-amount').value      = '';
    document.getElementById('txn-date').value        = '';
}

// Delete a transaction
async function deleteTransaction(id) {
    dbTransactions = dbTransactions.filter(t => t.id !== id);
    await saveTransactionsToDB();
    renderTransactionLedger();
}

// Set filter type and re-render
function setTxnFilter(type) {
    txnFilterType = type;
    ['all','income','expense'].forEach(t => {
        const btn = document.getElementById(`txn-filter-${t}`);
        if (!btn) return;
        btn.className = t === type
            ? 'px-3 py-1.5 bg-brand-500 text-slate-950 font-black text-[10px] uppercase'
            : 'px-3 py-1.5 text-slate-400 hover:text-white transition-all text-[10px] uppercase font-black';
    });
    renderTransactionLedger();
}

// Populate month filter dropdown from available transaction dates
function buildTxnMonthFilterDropdown() {
    const select = document.getElementById('txn-month-filter');
    if (!select) return;
    const months = [...new Set(dbTransactions.map(t => t.date.slice(0, 7)))].sort().reverse();
    const current = select.value;
    select.innerHTML = `<option value="all">All Months</option>` +
        months.map(m => {
            const [y, mo] = m.split('-');
            const label = new Date(parseInt(y), parseInt(mo) - 1, 1).toLocaleDateString('en-GB', { month: 'long', year: 'numeric' });
            return `<option value="${m}" ${m === current ? 'selected' : ''}>${label}</option>`;
        }).join('');
}

// Export visible transactions to CSV
function exportTransactionLedgerCSV() {
    const monthFilter = document.getElementById('txn-month-filter')?.value || 'all';
    let rows = [...dbTransactions].sort((a, b) => a.date.localeCompare(b.date));
    if (txnFilterType !== 'all') rows = rows.filter(t => t.type === txnFilterType);
    if (monthFilter !== 'all')   rows = rows.filter(t => t.date.startsWith(monthFilter));

    if (rows.length === 0) return alert("No transactions to export.");

    let balance = 0;
    const lines = [
        ['Date', 'Description', 'Category', 'Type', 'Amount (£)', 'Running Balance (£)', 'Source', 'Job Ref'].join(','),
        ...rows.map(t => {
            balance += t.type === 'income' ? t.amount : -t.amount;
            return [t.date, `"${t.description}"`, t.category, t.type, t.amount.toFixed(2), balance.toFixed(2), t.source, t.job_ref || ''].join(',');
        })
    ];

    const blob = new Blob([lines.join('\n')], { type: 'text/csv' });
    const url  = URL.createObjectURL(blob);
    const a    = document.createElement('a');
    a.href     = url;
    a.download = `PGS_Transactions_${new Date().toISOString().split('T')[0]}.csv`;
    a.click();
    URL.revokeObjectURL(url);
}

// Category badge colour map
const txnCatColors = {
    'Deposit Received': 'text-emerald-400 bg-emerald-500/10 border-emerald-500/20',
    'Job Income':       'text-emerald-400 bg-emerald-500/10 border-emerald-500/20',
    'Materials':        'text-brand-400 bg-brand-500/10 border-brand-500/20',
    'Hardware':         'text-sky-400 bg-sky-500/10 border-sky-500/20',
    'Tools':            'text-purple-400 bg-purple-500/10 border-purple-500/20',
    'Transport':        'text-amber-400 bg-amber-500/10 border-amber-500/20',
    'Marketing':        'text-pink-400 bg-pink-500/10 border-pink-500/20',
    'Other':            'text-slate-400 bg-slate-800 border-slate-700'
};

// Render the full transaction ledger table
function renderTransactionLedger() {
    buildTxnMonthFilterDropdown();

    const tbody      = document.getElementById('txn-ledger-tbody');
    const monthFilter = document.getElementById('txn-month-filter')?.value || 'all';
    if (!tbody) return;

    // Sort ascending for running balance calculation
    let rows = [...dbTransactions].sort((a, b) => a.date.localeCompare(b.date) || a.id.localeCompare(b.id));

    // Compute running balance on all rows first
    let runningBalance = 0;
    rows = rows.map(t => {
        runningBalance += t.type === 'income' ? t.amount : -t.amount;
        return { ...t, _balance: runningBalance };
    });

    // Then apply filters for display (but keep balance from full set)
    let display = [...rows];
    if (txnFilterType !== 'all') display = display.filter(t => t.type === txnFilterType);
    if (monthFilter !== 'all')   display = display.filter(t => t.date.startsWith(monthFilter));

    // Summary strip
    const totalIn  = display.filter(t => t.type === 'income').reduce((s, t) => s + t.amount, 0);
    const totalOut = display.filter(t => t.type === 'expense').reduce((s, t) => s + t.amount, 0);
    const net      = totalIn - totalOut;

    const el = id => document.getElementById(id);
    if (el('txn-total-in'))  el('txn-total-in').textContent  = `£${totalIn.toFixed(2)}`;
    if (el('txn-total-out')) el('txn-total-out').textContent = `£${totalOut.toFixed(2)}`;
    if (el('txn-net')) {
        el('txn-net').textContent  = `£${Math.abs(net).toFixed(2)}`;
        el('txn-net').className    = `font-black text-base ${net >= 0 ? 'text-emerald-400' : 'text-rose-400'}`;
    }

    if (display.length === 0) {
        tbody.innerHTML = `<tr><td colspan="6" class="text-center text-slate-500 italic py-10 text-xs">No transactions match the current filter.</td></tr>`;
        return;
    }

    // Render rows newest-first for the table display
    tbody.innerHTML = [...display].reverse().map(t => {
        const isIncome    = t.type === 'income';
        const amtColor    = isIncome ? 'text-emerald-400' : 'text-rose-400';
        const amtSign     = isIncome ? '+' : '-';
        const balColor    = t._balance >= 0 ? 'text-slate-200' : 'text-rose-400';
        const catClass    = txnCatColors[t.category] || txnCatColors['Other'];
        const isAuto      = t.source !== 'manual';
        const autoTag     = isAuto ? `<span class="ml-1 text-[9px] text-slate-600 font-bold uppercase">auto</span>` : '';
        const deleteBtn   = isAuto
            ? `<td class="py-2.5 w-6 text-center"><span class="text-slate-700 text-xs" title="Auto-generated — delete source record to remove">🔒</span></td>`
            : `<td class="py-2.5 w-6 text-center"><button onclick="deleteTransaction('${t.id}')" class="text-slate-600 hover:text-rose-400 transition-all font-black text-sm leading-none">×</button></td>`;

        return `
            <tr class="hover:bg-slate-900/40 transition-colors group">
                <td class="py-2.5 pr-3 text-slate-400 whitespace-nowrap font-mono text-[10px]">${t.date}</td>
                <td class="py-2.5 pr-3 text-white font-bold max-w-[160px]">
                    <span class="block truncate">${t.description}${autoTag}</span>
                    ${t.job_ref ? `<span class="text-[10px] text-slate-500">${t.job_ref}</span>` : ''}
                </td>
                <td class="py-2.5 pr-3">
                    <span class="px-2 py-0.5 rounded-lg border text-[10px] font-bold uppercase ${catClass} whitespace-nowrap">${t.category}</span>
                </td>
                <td class="py-2.5 pr-3 text-right font-black ${amtColor} whitespace-nowrap">${amtSign}£${t.amount.toFixed(2)}</td>
                <td class="py-2.5 text-right font-bold ${balColor} whitespace-nowrap text-[11px]">£${t._balance.toFixed(2)}</td>
                ${deleteBtn}
            </tr>
        `;
    }).join('');
}

// ─── END TRANSACTION LEDGER ENGINE ────────────────────────────────────────────

// Master financial calculation and render dispatcher
function calculateGlobalTurnoverLedgerComplianceSplits() {
    const taxYear  = getCurrentTaxYear();
    const now      = new Date();

    // Filter jobs in the current tax year
    const settledJobs = liveJobsPipeline.filter(j => {
        if (j.status !== 'Settled') return false;
        const d = new Date(j.created_at);
        return d >= taxYear.start && d <= taxYear.end;
    });

    // All-time settled for VAT rolling 12m check
    const rollingStart = new Date(now); rollingStart.setFullYear(rollingStart.getFullYear() - 1);
    const rollingSettled = liveJobsPipeline.filter(j => j.status === 'Settled' && new Date(j.created_at) >= rollingStart);
    const rollingGross = rollingSettled.reduce((s, j) => s + parseFloat(j.gross_revenue || 0), 0);

    // Pipeline value (booked but not settled)
    const pipelineJobs  = liveJobsPipeline.filter(j => j.status !== 'Settled' && j.status !== 'Completed' && j.deposit_paid);
    const pipelineValue = pipelineJobs.reduce((s, j) => s + parseFloat(j.gross_revenue || 0), 0);

    // Tax year gross + profit
    let grossRevenue = 0, totalCOGS = 0;
    settledJobs.forEach(j => {
        grossRevenue += parseFloat(j.gross_revenue || 0);
        totalCOGS    += estimateJobCOGS(j);
    });
    const netProfit   = grossRevenue - totalCOGS;
    const avgMargin   = grossRevenue > 0 ? (netProfit / grossRevenue * 100) : 0;
    const taxPot      = grossRevenue * 0.26;

    // Expenses total
    const expensesTotal = dbExpenses.reduce((s, e) => s + parseFloat(e.amount || 0), 0);

    // Self assessment deadline
    const saDeadlineYear = taxYear.end.getFullYear() + 1; // Jan 31 after tax year ends
    const saDeadline     = new Date(saDeadlineYear, 0, 31);
    const daysToSA       = Math.ceil((saDeadline - now) / (1000 * 60 * 60 * 24));

    // Monthly revenue breakdown (by calendar month of tax year)
    const monthlyTotals = new Array(12).fill(0);
    settledJobs.forEach(j => {
        const d = new Date(j.created_at);
        monthlyTotals[d.getMonth()] += parseFloat(j.gross_revenue || 0);
    });

    // ── Update KPI cards ──
    const el = id => document.getElementById(id);
    if (el('fin-gross-kpi'))      el('fin-gross-kpi').textContent      = `£${grossRevenue.toFixed(2)}`;
    if (el('fin-pipeline-kpi'))   el('fin-pipeline-kpi').textContent   = `Pipeline: £${pipelineValue.toFixed(2)}`;
    if (el('fin-profit-kpi'))     el('fin-profit-kpi').textContent     = `£${netProfit.toFixed(2)}`;
    if (el('fin-margin-kpi'))     el('fin-margin-kpi').textContent     = `Avg margin: ${avgMargin.toFixed(0)}%`;
    if (el('fin-tax-kpi'))        el('fin-tax-kpi').textContent        = `£${taxPot.toFixed(2)}`;
    if (el('fin-sa-countdown'))   el('fin-sa-countdown').textContent   = `SA deadline: ${daysToSA}d`;
    if (el('fin-expenses-kpi'))   el('fin-expenses-kpi').textContent   = `£${expensesTotal.toFixed(2)}`;
    if (el('fin-expenses-count')) el('fin-expenses-count').textContent = `${dbExpenses.length} entr${dbExpenses.length === 1 ? 'y' : 'ies'}`;

    // ── Tax year info panel ──
    if (el('fin-tax-year-label'))    el('fin-tax-year-label').textContent    = `${taxYear.label}`;
    if (el('fin-sa-deadline-label')) el('fin-sa-deadline-label').textContent = `31 Jan ${saDeadlineYear}`;
    if (el('fin-sa-days-label'))     el('fin-sa-days-label').textContent     = daysToSA > 0 ? `${daysToSA} days` : 'OVERDUE';

    // ── Threshold bars ──
    const vatThreshold = parseInt(dbConfigs['vat_threshold_pound']) || 90000;
    const vatPct = Math.min((rollingGross / vatThreshold) * 100, 100);
    if (el('fin-vat-progress-bar')) el('fin-vat-progress-bar').style.width = `${vatPct}%`;
    if (el('fin-vat-pct-label'))    el('fin-vat-pct-label').textContent     = `${vatPct.toFixed(0)}%`;

    const mtdThreshold = parseInt(dbConfigs['mtd_threshold_pound']) || 50000;
    const mtdPct = Math.min((grossRevenue / mtdThreshold) * 100, 100);
    if (el('fin-mtd-progress-bar')) el('fin-mtd-progress-bar').style.width = `${mtdPct}%`;
    if (el('fin-mtd-pct-label'))    el('fin-mtd-pct-label').textContent     = `${mtdPct.toFixed(0)}%`;

    // ── Charts & lists ──
    renderMonthlyRevenueChart(monthlyTotals, taxYear.label);
    renderJobMarginList(settledJobs);
    renderExpenseList();
    renderTransactionLedger();
}

// Render Master General Recap Calendar matrix block
function buildProductionScheduleCalendarGrid() {
    const grid = document.getElementById('calendar-grid-matrix-container');
    if (!grid) return;
    const weekdayLabels = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'];
    let calendarHTML = weekdayLabels.map(w => `<div class="text-slate-500 font-bold uppercase text-[10px] pb-2 border-b border-slate-800">${w}</div>`).join('');
    const baseAnchorDate = new Date();
    const currentWeekdayOffsetIndex = baseAnchorDate.getDay();
    const shiftToMondayIndexValue = currentWeekdayOffsetIndex === 0 ? -6 : 1 - currentWeekdayOffsetIndex;
    let calendarDaysIteratorInstance = new Date(baseAnchorDate);
    calendarDaysIteratorInstance.setDate(baseAnchorDate.getDate() + shiftToMondayIndexValue);

    for (let i = 0; i < 28; i++) {
        const isoStringDateMarker = calendarDaysIteratorInstance.toISOString().split('T')[0];
        const dayOfMonthNumericLabel = calendarDaysIteratorInstance.getDate();
        const absoluteDayOfWeekIndex = calendarDaysIteratorInstance.getDay(); 
        const isPrepDayMatched = liveJobsPipeline.some(j => j.prep_date === isoStringDateMarker && j.status !== 'Settled');
        const isInstallDayMatched = liveJobsPipeline.some(j => j.install_date && j.install_date.startsWith(isoStringDateMarker) && j.status !== 'Settled');
        let cellStylingTokens = "bg-slate-900/40 text-slate-400 border border-slate-800/60";
        let statusDescriptorTagLabelText = "";

        if (absoluteDayOfWeekIndex === 6) {
            cellStylingTokens = "bg-rose-950/20 text-rose-500/60 border border-rose-900/40 font-bold";
            statusDescriptorTagLabelText = `<span class="block text-[7px] text-rose-400 font-bold mt-1 uppercase">SAT BLOCK</span>`;
        } else if (isInstallDayMatched) {
            cellStylingTokens = "bg-brand-500/10 text-brand-400 border border-brand-500/30 font-black";
            statusDescriptorTagLabelText = `<span class="block text-[8px] text-brand-400 mt-1 font-bold">INSTALL</span>`;
        } else if (isPrepDayMatched) {
            cellStylingTokens = "bg-amber-500/10 text-amber-400 border border-amber-500/30 font-black";
            statusDescriptorTagLabelText = `<span class="block text-[8px] text-amber-400 mt-1 font-bold">PREP RUN</span>`;
        }
        calendarHTML += `<div class="p-3 rounded-xl min-h-[58px] flex flex-col justify-between items-center ${cellStylingTokens}"><span class="text-xs font-bold">${dayOfMonthNumericLabel}</span>${statusDescriptorTagLabelText}</div>`;
        calendarDaysIteratorInstance.setDate(calendarDaysIteratorInstance.getDate() + 1);
    }
    grid.innerHTML = calendarHTML;
}

// Toggle Deposit Flag and Advance Pipeline Stage Node status
async function toggleDepositStatusPaidRegistryFlag(id, currentFlag) {
    const updateData = { deposit_paid: !currentFlag };
    if (!currentFlag) updateData.status = 'Booked';
    await supabaseClient.from('job_ledger').update(updateData).eq('id', id);

    // Auto-log deposit transaction when marking as paid
    if (!currentFlag) {
        const job = liveJobsPipeline.find(j => j.id === id);
        if (job) {
            const depositAmt = parseFloat(job.deposit_amount || 150);
            await addAutoTransaction(
                `Deposit received — ${job.customer_name} (#100${job.quote_number})`,
                depositAmt, 'income', 'Deposit Received',
                `#100${job.quote_number}`
            );
        }
    }

    await syncStateWithDatabaseCluster();
    openActiveJobDrilldownActionView(id);
}

// ── Payment QR Checkout Modal ──
let activeQRJobId = null;

function showDepositQRModal(job) {
    activeQRJobId = job.id;

    const bacsName     = dbConfigs['bacs_account_name']   || 'Potteries Garage Solutions';
    const bacsSortCode = dbConfigs['bacs_sort_code']      || '00-00-00';
    const bacsAccNo    = dbConfigs['bacs_account_number'] || '00000000';
    const depositAmt   = parseFloat(job.deposit_amount || 150).toFixed(2);
    const payRef       = `PGS100${job.quote_number}`;

    document.getElementById('qr-account-name').textContent   = bacsName;
    document.getElementById('qr-sort-code').textContent      = bacsSortCode;
    document.getElementById('qr-account-number').textContent = bacsAccNo;
    document.getElementById('qr-amount').textContent         = `£${depositAmt}`;
    document.getElementById('qr-reference').textContent      = payRef;

    const qrPayload = `${bacsName}\nSort: ${bacsSortCode}\nAcc: ${bacsAccNo}\nAmt: ${depositAmt}\nRef: ${payRef}`;

    const container = document.getElementById('payment-qr-canvas');
    container.innerHTML = '';
    new QRCode(container, {
        text: qrPayload,
        width: 200,
        height: 200,
        colorDark: '#000000',
        colorLight: '#ffffff',
        correctLevel: QRCode.CorrectLevel.M,
    });

    document.getElementById('payment-qr-modal').classList.remove('hidden');
}

function copyQRField(fieldId, btnId) {
    const text = document.getElementById(fieldId).textContent.trim();
    const btn  = document.getElementById(btnId);
    navigator.clipboard.writeText(text).then(() => {
        btn.textContent = '✓';
        btn.classList.add('text-emerald-400', 'border-emerald-500/60');
        setTimeout(() => {
            btn.textContent = 'Copy';
            btn.classList.remove('text-emerald-400', 'border-emerald-500/60');
        }, 1500);
    });
}

function copyAllQRDetails() {
    const name   = document.getElementById('qr-account-name').textContent.trim();
    const sort   = document.getElementById('qr-sort-code').textContent.trim();
    const acc    = document.getElementById('qr-account-number').textContent.trim();
    const amount = document.getElementById('qr-amount').textContent.trim();
    const ref    = document.getElementById('qr-reference').textContent.trim();
    const text   = `Account Name: ${name}\nSort Code: ${sort}\nAccount No: ${acc}\nAmount: ${amount}\nReference: ${ref}`;
    const btn    = document.getElementById('copy-btn-all');
    navigator.clipboard.writeText(text).then(() => {
        btn.textContent = '✓ Copied to Clipboard';
        btn.classList.add('text-emerald-400', 'border-emerald-500/40', 'bg-emerald-500/10');
        setTimeout(() => {
            btn.textContent = 'Copy All Details';
            btn.classList.remove('text-emerald-400', 'border-emerald-500/40', 'bg-emerald-500/10');
        }, 2000);
    });
}

function showDepositQRForJob(jobId) {
    const job = liveJobsPipeline.find(j => j.id === jobId);
    if (job) showDepositQRModal(job);
}

function closePaymentQRModal() {
    document.getElementById('payment-qr-modal').classList.add('hidden');
    activeQRJobId = null;
}

async function confirmDepositReceivedFromQR() {
    if (!activeQRJobId) return closePaymentQRModal();
    const job = liveJobsPipeline.find(j => j.id === activeQRJobId);
    if (job && !job.deposit_paid) {
        await toggleDepositStatusPaidRegistryFlag(job.id, false);
    }
    closePaymentQRModal();
    showToastNotification('Deposit marked as received ✓');
}

// Generate Comprehensive Invoice Documents Matching Team Identity Sourcing Matrix
async function triggerInvoiceDocumentPDFDownload(jobId) {
    const job = liveJobsPipeline.find(j => j.id === jobId);
    if (!job) return alert("System trace error: Job record target could not be verified.");

    const { jsPDF } = window.jspdf;
    const doc = new jsPDF({ orientation: 'p', unit: 'mm', format: 'a4' });

    doc.setFillColor(9, 13, 22);
    doc.rect(0, 0, 210, 42, 'F');

    doc.setFont("helvetica", "bold");
    doc.setFontSize(22);
    doc.setTextColor(255, 135, 0); 
    doc.text("POTTERIES GARAGE SOLUTIONS", 15, 20);

    doc.setFontSize(9);
    doc.setFont("helvetica", "normal");
    doc.setTextColor(156, 163, 175);
    doc.text("PREMIUM STRUCTURAL TIMBER GARAGE SHELVING", 15, 30);
    doc.text(`Stoke-on-Trent, Staffordshire | ${dbConfigs['business_email'] || 'info@potteriesgaragesolutions.co.uk'}`, 15, 35);

    doc.setTextColor(15, 23, 42); 
    doc.setFontSize(11);
    doc.setFont("helvetica", "bold");
    doc.text(`COMMERCIAL PROPOSAL SUMMARY — ORDER #PGS100${job.quote_number}`, 15, 54);

    doc.setFont("helvetica", "normal");
    doc.setFontSize(10);
    doc.text(`Customer Name:   ${job.customer_name}`, 15, 64);
    doc.text(`Site Address:    ${job.installation_address}`, 15, 71);
    doc.text(`Install Date:    ${job.install_date || 'To Be Scheduled'}`, 15, 78);
    doc.text(`Prep Run Week:   ${job.prep_date || 'To Be Scheduled'}`, 15, 85);

    doc.setDrawColor(226, 232, 240);
    doc.line(15, 93, 195, 93);

    doc.setFont("helvetica", "bold");
    doc.text("Itemized Structural Scope of Build Assemblies:", 15, 102);
    
    doc.setFont("helvetica", "normal");
    let currentYPositionOffset = 112;

    let productItemsArray = [];
    try { 
        productItemsArray = typeof job.job_items === 'string' ? JSON.parse(job.job_items) : job.job_items || []; 
    } catch(e) { 
        productItemsArray = []; 
    }

    if(productItemsArray.length === 0) {
        doc.text("• Custom Built Modular Component Package", 20, currentYPositionOffset);
        doc.text(`£${parseFloat(job.gross_revenue).toFixed(2)}`, 195, currentYPositionOffset, { align: 'right' });
        currentYPositionOffset += 10;
    } else {
        productItemsArray.forEach((item, index) => {
            doc.setFont("helvetica", "bold");
            doc.setFontSize(10);
            doc.setTextColor(15, 23, 42);
            doc.text(`${index + 1}. ${item.name}`, 20, currentYPositionOffset);
            doc.text(`£${parseFloat(item.workingPrice || job.gross_revenue).toFixed(2)}`, 195, currentYPositionOffset, { align: 'right' });

            doc.setFont("helvetica", "normal");
            doc.setFontSize(9);
            doc.setTextColor(100, 116, 139);
            doc.text(`${item.width_mm}mm W × ${item.depth_mm}mm D × ${item.height_mm}mm H  |  ${item.shelves_count} Shelf Tier${item.shelves_count !== 1 ? 's' : ''}`, 24, currentYPositionOffset + 5);

            let subLines = 1;
            if (item.shelf_height_mm != null) {
                doc.text(`Work Surface Height: ${item.shelf_height_mm}mm from floor`, 24, currentYPositionOffset + 11);
                subLines = 2;
            }

            doc.setFontSize(10);
            doc.setTextColor(15, 23, 42);
            currentYPositionOffset += 8 + subLines * 6;
        });
    }

    doc.setDrawColor(226, 232, 240);
    doc.line(15, currentYPositionOffset + 2, 195, currentYPositionOffset + 2);

    doc.setFont("helvetica", "bold");
    doc.setFontSize(12);
    doc.text(`Total Guaranteed Project Quote Price:`, 15, currentYPositionOffset + 12);
    doc.setTextColor(16, 185, 129); 
    doc.text(`£${parseFloat(job.gross_revenue).toFixed(2)}`, 195, currentYPositionOffset + 12, { align: 'right' });

    // ── BACS Payment Details block ──
    const bacsBoxTopY = currentYPositionOffset + 22;
    const bacsName    = dbConfigs['bacs_account_name']   || 'Potteries Garage Solutions';
    const bacsSortCode = dbConfigs['bacs_sort_code']     || '00-00-00';
    const bacsAccNo   = dbConfigs['bacs_account_number'] || '00000000';
    const depositAmt  = parseFloat(job.deposit_amount || 150).toFixed(2);
    const payRef      = `PGS100${job.quote_number}`;

    doc.setFillColor(236, 253, 245);
    doc.rect(15, bacsBoxTopY, 180, 34, 'F');
    doc.setDrawColor(52, 211, 153);
    doc.rect(15, bacsBoxTopY, 180, 34, 'D');

    doc.setFontSize(9);
    doc.setTextColor(6, 95, 70);
    doc.setFont("helvetica", "bold");
    doc.text("BACS DEPOSIT PAYMENT DETAILS:", 19, bacsBoxTopY + 7);
    doc.setFont("helvetica", "normal");
    doc.text(`Account Name:   ${bacsName}`, 19, bacsBoxTopY + 14);
    doc.text(`Sort Code:       ${bacsSortCode}       Account No:  ${bacsAccNo}`, 19, bacsBoxTopY + 20);
    doc.setFont("helvetica", "bold");
    doc.text(`Deposit Amount: £${depositAmt}     Payment Reference: ${payRef}`, 19, bacsBoxTopY + 27);

    // ── Deposit terms box ──
    const termsBoxTopY = bacsBoxTopY + 40;
    doc.setFillColor(254, 243, 199); 
    doc.rect(15, termsBoxTopY, 180, 26, 'F');
    doc.setDrawColor(251, 191, 36); 
    doc.rect(15, termsBoxTopY, 180, 26, 'D');

    doc.setFontSize(9);
    doc.setTextColor(180, 83, 9); 
    doc.setFont("helvetica", "bold");
    doc.text("DEPOSIT & BOOKING TERMS:", 19, termsBoxTopY + 6);
    doc.setFont("helvetica", "normal");
    doc.text(`A £${depositAmt} deposit secures your install slot. Remaining balance due on completion. Use ref: ${payRef}`, 19, termsBoxTopY + 12);
    doc.text("Site must be clear of obstructions. Masonry walls must be suitable for anchor fixings.", 19, termsBoxTopY + 18);

    doc.save(`PGS_Commercial_Proposal_Quote_100${job.quote_number}.pdf`);
}

// Context-aware PDF printer — uses the currently open drilldown job, falls back to most recent
let activeJobDrilldownId = null;

triggerInvoiceDocumentPDFDownloadFromActiveView = function() {
    if (liveJobsPipeline.length === 0) return alert("Save a configuration model record item first to build invoices files.");
    const targetId = activeJobDrilldownId || liveJobsPipeline[0].id;
    triggerInvoiceDocumentPDFDownload(targetId);
}

// Keys managed by dedicated UIs — skip in the raw constants form
const MANAGED_CONFIG_KEYS = new Set(['blocked_dates','pgs_product_recipes','pgs_expense_log','pgs_transaction_log','available_weekdays','available_time_slots','work_day_start','work_day_end','slot_duration_hours','hours_per_bay']);

// Generate Admin Configurations Field Matrix Block
function renderSystemSettingsCRUDForms() {
    const grid = document.getElementById('admin-configs-inputs-target-grid'); if (!grid) return;
    grid.innerHTML = Object.keys(dbConfigs)
        .filter(key => !MANAGED_CONFIG_KEYS.has(key))
        .map(key => `<div class="space-y-1"><label class="block text-[10px] uppercase text-slate-500 font-bold">${key.replace(/_/g, ' ')}</label><input type="text" id="config-input-field-${key}" value="${dbConfigs[key]}" class="w-full bg-slate-900 border border-slate-700 rounded-xl p-3 text-white text-xs font-bold outline-none"></div>`).join('');
    populateSchedulingConfigInputs();
}

// Save Admin Constants directly to cloud clusters
async function commitGlobalConfigModificationChanges() {
    const rows = [];
    for (let key of Object.keys(dbConfigs)) {
        const el = document.getElementById('config-input-field-' + key);
        if (el) rows.push({ key, value: el.value });
    }
    if (rows.length) {
        const { error } = await supabaseClient
            .from('configuration_ledger')
            .upsert(rows, { onConflict: 'key' });
        if (error) { alert('Save failed: ' + error.message); return; }
        rows.forEach(r => { dbConfigs[r.key] = r.value; });
    }
    alert('Variables locked.');
    await syncStateWithDatabaseCluster();
}

// Visual Alert Timing Checkpoints Loop
function configureSystemIntervalAlarms() {
    // Only fire the workshop alarm on Saturdays
    setInterval(() => {
        const now = new Date();
        const timeMarkerString = now.toTimeString().split(' ')[0];
        const isSaturday = now.getDay() === 6;
        if (isSaturday && timeMarkerString.startsWith("11:30")) document.getElementById('alarm-banner').classList.remove('hidden');
    }, 1000);
}

// Toast Notification Helper
function showToastNotification(message, durationMs = 3000) {
    let toastEl = document.getElementById('pgs-toast');
    if (!toastEl) {
        toastEl = document.createElement('div');
        toastEl.id = 'pgs-toast';
        toastEl.style.cssText = [
            'position:fixed','bottom:24px','left:50%','transform:translateX(-50%) translateY(20px)',
            'background:#FF8700','color:#0f172a','padding:10px 20px',
            'border-radius:12px','font-size:12px','font-weight:900',
            'letter-spacing:0.05em','text-transform:uppercase',
            'box-shadow:0 8px 30px rgba(0,0,0,0.4)','z-index:9999',
            'opacity:0','transition:all 0.25s ease','pointer-events:none',
            'white-space:nowrap','max-width:90vw','text-align:center'
        ].join(';');
        document.body.appendChild(toastEl);
    }
    toastEl.textContent = message;
    requestAnimationFrame(() => {
        toastEl.style.opacity = '1';
        toastEl.style.transform = 'translateX(-50%) translateY(0)';
    });
    clearTimeout(toastEl._timeout);
    toastEl._timeout = setTimeout(() => {
        toastEl.style.opacity = '0';
        toastEl.style.transform = 'translateX(-50%) translateY(20px)';
    }, durationMs);
}

// Redraw the revenue chart if the window is resized while on the finance tab
window.addEventListener('resize', () => {
    const finTab = document.getElementById('finance-tab');
    if (finTab && !finTab.classList.contains('hidden')) {
        calculateGlobalTurnoverLedgerComplianceSplits();
    }
});

function toggleElement(id) { document.getElementById(id).classList.toggle('hidden'); }
function dismissAlarm() { document.getElementById('alarm-banner').classList.add('hidden'); }

// ─── SCHEDULING ENGINE v2 ─────────────────────────────────────────────────────
// Design: admin configures available weekdays + explicit time slots.
// Each slot = one booking. Conflict = exact slot already has a non-cancelled job.
// No duration math, no time-window overlap, no vacuous-truth bugs.

// Parse any time string → decimal hours  ("08:30 AM"→8.5, "14:00"→14.0)
function parseTimeToDecimalHours(timeStr) {
    if (!timeStr) return 0;
    const clean = timeStr.trim().toUpperCase();
    const isPM  = clean.includes('PM');
    const isAM  = clean.includes('AM');
    const parts = clean.replace(/\s*(AM|PM)\s*/i, '').split(':');
    let h = parseInt(parts[0]) || 0;
    const m = parseInt(parts[1]) || 0;
    if (isPM && h !== 12) h += 12;
    if (isAM && h === 12) h = 0;
    return h + m / 60;
}

// Hours → "2h 30m" label
function formatDurationLabel(hours) {
    const h = Math.floor(hours);
    const m = Math.round((hours - h) * 60);
    if (h === 0) return `${m}m`;
    if (m === 0) return `${h}h`;
    return `${h}h ${m}m`;
}

// "08:00" (24h) → "08:00 AM" display label
function formatSlotLabel(hhmm) {
    const [h, m] = hhmm.split(':').map(Number);
    const ampm = h < 12 ? 'AM' : 'PM';
    const h12  = h === 0 ? 12 : h > 12 ? h - 12 : h;
    return `${String(h12).padStart(2,'0')}:${String(m).padStart(2,'0')} ${ampm}`;
}

// Any time string → "HH:MM" 24h (e.g. "08:30 AM"→"08:30")
function toHHMM(timeStr) {
    const dec = parseTimeToDecimalHours(timeStr);
    const h = Math.floor(dec);
    const m = Math.round((dec - h) * 60);
    return `${String(h).padStart(2,'0')}:${String(m).padStart(2,'0')}`;
}

// Estimated install duration for display badge only (no longer used for conflict logic)
function calculateInstallDurationHours(modules) {
    if (!modules || modules.length === 0) return 0;
    const hoursPerBay = parseFloat(dbConfigs['hours_per_bay'] || 1.5);
    let total = 0;
    modules.forEach(item => {
        const recipe = dbRecipes[item.id];
        const mult   = (recipe && recipe.heavy_duty) ? 1.3 : 1.0;
        total += hoursPerBay * mult;
    });
    return total;
}

// ── Available days / slots config ─────────────────────────────────────────────

function getAvailableWeekdays() {
    const val = dbConfigs['available_weekdays'];
    if (val === undefined || val === null || val === '') return new Set([1,2,3,4,5]);
    return new Set(String(val).split(',').map(n => parseInt(n.trim())).filter(n => !isNaN(n)));
}

function getAvailableTimeSlots() {
    const val = dbConfigs['available_time_slots'];
    if (!val) return ['08:00','11:00','14:00'];
    return String(val).split(',').map(t => t.trim()).filter(t => /^\d{1,2}:\d{2}$/.test(t)).sort();
}

// Check whether a specific HH:MM slot on a date is already booked.
// Timed jobs: 6-min tolerance exact match.
// Timeless (legacy) jobs: each one claims the first N unclaimed configured slots
// in ascending order, so they are never invisible to the slot picker or save guard.
function isSlotBooked(dateStr, slotHHMM, excludeJobId = null) {
    const dayJobs = liveJobsPipeline.filter(j =>
        j.install_date && j.install_date.startsWith(dateStr) &&
        j.status !== 'Cancelled' && j.id !== excludeJobId
    );
    if (dayJobs.length === 0) return false;

    const slotDec = parseTimeToDecimalHours(slotHHMM);

    // 1. Exact timed conflict (6-min tolerance)
    const hasTimedConflict = dayJobs.some(j => {
        const tp = (j.install_date.split(' at ')[1] || '').trim();
        return tp && Math.abs(parseTimeToDecimalHours(tp) - slotDec) < 0.1;
    });
    if (hasTimedConflict) return true;

    // 2. Timeless jobs claim the first N unclaimed configured slots
    const untimedCount = dayJobs.filter(j => !j.install_date.includes(' at ')).length;
    if (untimedCount === 0) return false;

    const configuredSlots = getAvailableTimeSlots();
    // Which configured slots are already claimed by timed jobs?
    const timedClaimed = new Set(configuredSlots.filter(s => {
        const sDec = parseTimeToDecimalHours(s);
        return dayJobs.some(j => {
            const tp = (j.install_date.split(' at ')[1] || '').trim();
            return tp && Math.abs(parseTimeToDecimalHours(tp) - sDec) < 0.1;
        });
    }));
    // Remaining slots (in config order) are claimed by timeless jobs first-come-first-served
    const reservedByUntimed = configuredSlots.filter(s => !timedClaimed.has(s)).slice(0, untimedCount);
    return reservedByUntimed.includes(slotHHMM);
}

// Returns null | 'blocked' | 'unavailable_day' | 'slot_booked'
function findSchedulingConflict(dateStr, timeStr, _modules, excludeJobId = null) {
    if (!dateStr || !timeStr) return null;
    if (getBlockedDates().includes(dateStr)) return 'blocked';
    const dow = new Date(dateStr + 'T12:00:00').getDay();
    if (!getAvailableWeekdays().has(dow)) return 'unavailable_day';
    // Day-capacity check: total active jobs on this day must not reach slot count.
    // Covers legacy jobs stored without a specific time.
    const maxSlots = getAvailableTimeSlots().length;
    if (maxSlots > 0) {
        const totalOnDay = liveJobsPipeline.filter(j =>
            j.install_date && j.install_date.startsWith(dateStr) &&
            j.status !== 'Cancelled' && j.id !== excludeJobId
        ).length;
        if (totalOnDay >= maxSlots) return 'slot_booked';
    }
    // Slot-level check: exact time collision within 6 minutes
    if (isSlotBooked(dateStr, toHHMM(timeStr), excludeJobId)) return 'slot_booked';
    return null;
}

// ── Blocked dates ──────────────────────────────────────────────────────────────

function getBlockedDates() {
    try { return JSON.parse(dbConfigs['blocked_dates'] || '[]'); } catch(e) { return []; }
}

async function saveBlockedDates(arr) {
    await upsertConfig('blocked_dates', JSON.stringify(arr));
}

function addBlockedDateFromInput() {
    const input = document.getElementById('blocked-date-input');
    if (!input || !input.value) return;
    const arr = getBlockedDates();
    if (!arr.includes(input.value)) {
        arr.push(input.value);
        arr.sort();
        saveBlockedDates(arr).then(() => {
            renderBlockedDatesManagementPanel();
            buildModalInteractiveCalendarGrid();
            showToastNotification(`${input.value} blocked ✓`);
        });
    }
    input.value = '';
}

async function removeBlockedDate(dateStr) {
    const arr = getBlockedDates().filter(d => d !== dateStr);
    await saveBlockedDates(arr);
    renderBlockedDatesManagementPanel();
    buildModalInteractiveCalendarGrid();
    showToastNotification(`${dateStr} unblocked ✓`);
}

function renderBlockedDatesManagementPanel() {
    const container = document.getElementById('blocked-dates-list');
    if (!container) return;
    const arr = getBlockedDates().sort();
    if (arr.length === 0) {
        container.innerHTML = `<p class="text-slate-500 text-xs italic">No blocked dates set.</p>`;
        return;
    }
    container.innerHTML = arr.map(d => {
        const formatted = new Date(d + 'T12:00:00').toLocaleDateString('en-GB', { weekday:'short', day:'numeric', month:'short', year:'numeric' });
        return `
        <div class="flex items-center justify-between bg-slate-900 px-3 py-2 rounded-xl border border-rose-900/30 text-xs">
            <div>
                <span class="font-bold text-rose-300 block">${formatted}</span>
                <span class="text-slate-500 text-[10px] font-mono">${d}</span>
            </div>
            <button onclick="removeBlockedDate('${d}')" class="text-slate-600 hover:text-rose-400 font-black text-lg leading-none ml-3 transition-all">×</button>
        </div>`;
    }).join('');
}

// ── Slot picker — intake modal ────────────────────────────────────────────────

function renderModalTimeSlots(dateStr) {
    const container = document.getElementById('modal-time-slot-picker-grid');
    if (!container) return;

    const slots    = getAvailableTimeSlots();
    const isBlocked = dateStr && getBlockedDates().includes(dateStr);
    const dow       = dateStr ? new Date(dateStr + 'T12:00:00').getDay() : -1;
    const dayOk     = dow >= 0 && getAvailableWeekdays().has(dow);

    // Duration badge
    const dur = calculateInstallDurationHours(activeBuildModulesListArray);
    const badge = document.getElementById('modal-duration-estimate');
    if (badge) {
        if (dur > 0) { badge.textContent = `Est. ${formatDurationLabel(dur)}`; badge.classList.remove('hidden'); }
        else badge.classList.add('hidden');
    }

    const reset = msg => {
        container.innerHTML = msg;
        container.style.gridTemplateColumns = '';
    };

    if (!dateStr)    return reset(`<p class="col-span-3 text-slate-500 text-xs italic text-center py-2">Select an installation date above to see available slots.</p>`);
    if (isBlocked)   return reset(`<p class="col-span-3 text-rose-400 text-xs font-bold text-center py-3 bg-rose-950/30 rounded-xl border border-rose-900/40">🔒 This day is blocked out — choose another date.</p>`);
    if (!dayOk)      return reset(`<p class="col-span-3 text-slate-500 text-xs text-center py-3 bg-slate-900/50 rounded-xl border border-slate-800">This day is not available for bookings.</p>`);
    if (!slots.length) return reset(`<p class="col-span-3 text-amber-400 text-xs font-bold text-center py-2">No time slots configured — add slots in Scheduling settings.</p>`);

    container.style.gridTemplateColumns = `repeat(${Math.min(slots.length, 3)}, minmax(0, 1fr))`;

    const selDec = selectedModalIntakeTimeStr ? parseTimeToDecimalHours(selectedModalIntakeTimeStr) : -1;

    container.innerHTML = slots.map(hhmm => {
        const label   = formatSlotLabel(hhmm);
        const booked  = isSlotBooked(dateStr, hhmm);
        const selMatch = selDec >= 0 && Math.abs(parseTimeToDecimalHours(hhmm) - selDec) < 0.1;

        if (booked) return `
            <div class="p-2.5 bg-slate-950 border border-rose-900/40 rounded-xl text-xs pointer-events-none select-none">
                <span class="text-rose-600 font-black line-through block">${label}</span>
                <span class="block text-[9px] mt-0.5 text-rose-800 font-bold uppercase">🔒 Taken</span>
            </div>`;
        if (selMatch) return `
            <button onclick="setIntakeBookingTime('${label}')" class="p-2.5 bg-brand-500 border border-brand-400 text-slate-950 font-black rounded-xl text-xs shadow-lg">
                ${label}<span class="block text-[9px] mt-0.5 font-bold opacity-75">✓ Selected</span>
            </button>`;
        return `
            <button onclick="setIntakeBookingTime('${label}')" class="p-2.5 bg-slate-900 border border-slate-700 text-slate-200 font-bold hover:border-brand-500 hover:bg-slate-800 rounded-xl text-xs transition-all">
                ${label}
            </button>`;
    }).join('');
}

// ── Slot picker — edit modal ──────────────────────────────────────────────────

function renderEditModalTimeSlots(dateStr) {
    const container = document.getElementById('edit-time-slot-picker-grid');
    if (!container) return;

    const slots    = getAvailableTimeSlots();
    const isBlocked = dateStr && getBlockedDates().includes(dateStr);
    const dow       = dateStr ? new Date(dateStr + 'T12:00:00').getDay() : -1;
    const dayOk     = dow >= 0 && getAvailableWeekdays().has(dow);

    const reset = msg => {
        container.innerHTML = msg;
        container.style.gridTemplateColumns = '';
    };

    if (!dateStr)    return reset(`<p class="col-span-3 text-slate-500 text-xs italic text-center py-2">Select an install date below to see available slots.</p>`);
    if (isBlocked)   return reset(`<p class="col-span-3 text-rose-400 text-xs font-bold text-center py-3 bg-rose-950/30 rounded-xl border border-rose-900/40">🔒 This day is blocked out — choose another date.</p>`);
    if (!dayOk)      return reset(`<p class="col-span-3 text-slate-500 text-xs text-center py-3 bg-slate-900/50 rounded-xl border border-slate-800">This day is not available for bookings.</p>`);
    if (!slots.length) return reset(`<p class="col-span-3 text-amber-400 text-xs font-bold text-center py-2">No time slots configured — add slots in Scheduling settings.</p>`);

    container.style.gridTemplateColumns = `repeat(${Math.min(slots.length, 3)}, minmax(0, 1fr))`;

    const selDec = editJobTimeStr ? parseTimeToDecimalHours(editJobTimeStr) : -1;

    container.innerHTML = slots.map(hhmm => {
        const label   = formatSlotLabel(hhmm);
        const booked  = isSlotBooked(dateStr, hhmm, editingJobId);
        const selMatch = selDec >= 0 && Math.abs(parseTimeToDecimalHours(hhmm) - selDec) < 0.1;

        if (booked) return `
            <div class="p-2.5 bg-slate-950 border border-rose-900/40 rounded-xl text-xs pointer-events-none select-none">
                <span class="text-rose-600 font-black line-through block">${label}</span>
                <span class="block text-[9px] mt-0.5 text-rose-800 font-bold uppercase">🔒 Taken</span>
            </div>`;
        if (selMatch) return `
            <button onclick="setEditJobTime('${label}')" class="p-2.5 bg-brand-500 border border-brand-400 text-slate-950 font-black rounded-xl text-xs shadow-lg">
                ${label}<span class="block text-[9px] mt-0.5 font-bold opacity-75">✓ Selected</span>
            </button>`;
        return `
            <button onclick="setEditJobTime('${label}')" class="p-2.5 bg-slate-900 border border-slate-700 text-slate-200 font-bold hover:border-brand-500 hover:bg-slate-800 rounded-xl text-xs transition-all">
                ${label}
            </button>`;
    }).join('');
}

// ── Admin config UI ───────────────────────────────────────────────────────────

function renderAvailDaysConfig() {
    const el = document.getElementById('avail-days-grid');
    if (!el) return;
    const avail = getAvailableWeekdays();
    const days = [{i:1,n:'Mon'},{i:2,n:'Tue'},{i:3,n:'Wed'},{i:4,n:'Thu'},{i:5,n:'Fri'},{i:6,n:'Sat'},{i:0,n:'Sun'}];
    el.innerHTML = days.map(d => `
        <button onclick="toggleAvailDay(${d.i})" class="${avail.has(d.i) ? 'bg-brand-500 text-slate-950 font-black shadow' : 'bg-slate-900 border border-slate-700 text-slate-400 font-bold'} px-4 py-2.5 rounded-xl text-xs uppercase tracking-wide transition-all">
            ${d.n}
        </button>`).join('');
}

function toggleAvailDay(idx) {
    const avail = getAvailableWeekdays();
    if (avail.has(idx)) avail.delete(idx); else avail.add(idx);
    dbConfigs['available_weekdays'] = [...avail].sort((a,b)=>a-b).join(',');
    renderAvailDaysConfig();
    _debouncedSaveScheduling();
}

function renderTimeSlotsConfigList() {
    const el = document.getElementById('time-slots-config-list');
    if (!el) return;
    const slots = getAvailableTimeSlots();
    if (!slots.length) { el.innerHTML = `<p class="text-slate-500 text-xs italic">No slots configured.</p>`; return; }
    el.innerHTML = slots.map(t => `
        <div class="flex items-center justify-between bg-slate-900 px-3 py-2 rounded-xl border border-slate-800 text-xs">
            <div>
                <span class="font-black text-white">${formatSlotLabel(t)}</span>
                <span class="text-slate-500 ml-2">${t}</span>
            </div>
            <button onclick="removeConfigTimeSlot('${t}')" class="text-slate-600 hover:text-rose-400 font-black text-lg leading-none transition-all ml-3">×</button>
        </div>`).join('');
}

function addConfigTimeSlot() {
    const input = document.getElementById('new-time-slot-input');
    if (!input || !input.value) return;
    const slots = getAvailableTimeSlots();
    if (!slots.includes(input.value)) {
        slots.push(input.value); slots.sort();
        dbConfigs['available_time_slots'] = slots.join(',');
        renderTimeSlotsConfigList();
        _debouncedSaveScheduling();
    }
    input.value = '';
}

function removeConfigTimeSlot(hhmm) {
    dbConfigs['available_time_slots'] = getAvailableTimeSlots().filter(t => t !== hhmm).join(',');
    renderTimeSlotsConfigList();
    _debouncedSaveScheduling();
}

async function saveSchedulingConfig() {
    try {
        await Promise.all([
            upsertConfig('available_weekdays',  dbConfigs['available_weekdays']  ?? '1,2,3,4,5'),
            upsertConfig('available_time_slots', dbConfigs['available_time_slots'] ?? '08:00,11:00,14:00'),
            upsertConfig('hours_per_bay',        dbConfigs['hours_per_bay']        ?? '1.5'),
        ]);
        showToastNotification('Scheduling settings saved ✓');
    } catch (e) {
        console.error('saveSchedulingConfig failed:', e.message);
        showToastNotification('Save failed ❌ — check console');
    }
}

function populateSchedulingConfigInputs() {
    renderAvailDaysConfig();
    renderTimeSlotsConfigList();
    renderBlockedDatesManagementPanel();
}

// ─── END SCHEDULING ENGINE v2 ─────────────────────────────────────────────────

// Permanent Database Sync Pipeline for Product Recipe Modifications
async function saveProductRecipeConfiguration(productId) {
    // 1. Grab the input elements matching the specific product row ID
    const baseTimberInput = document.getElementById(`recipe-base-timber-${productId}`);
    const shelfModifierInput = document.getElementById(`recipe-shelf-modifier-${productId}`);
    const baseHardwareInput = document.getElementById(`recipe-base-hardware-${productId}`);

    if (!baseTimberInput || !shelfModifierInput || !baseHardwareInput) {
        if (typeof showToast === "function") {
            showToast("Error: Missing input elements for this product recipe.", 3000);
        }
        return;
    }

    // 2. Extract values and sanitize them into clean numerical data types
    const updatedBaseTimber = parseFloat(baseTimberInput.value) || 0;
    const updatedShelfModifier = parseFloat(shelfModifierInput.value) || 0;
    const updatedBaseHardware = parseInt(baseHardwareInput.value, 10) || 0;

    try {
        // 3. Execute update statement on the Supabase cluster backend
        const { error } = await supabaseClient
            .from('products')
            .update({
                base_timber_factor: updatedBaseTimber,
                shelf_tier_modifier_price: updatedShelfModifier,
                base_hardware_count: updatedBaseHardware
            })
            .eq('id', productId);

        if (error) throw error;

        // 4. Mirror changes to global memory arrays so all UI panels update instantly
        const productIndex = dbProducts.findIndex(p => p.id === productId);
        if (productIndex !== -1) {
            dbProducts[productIndex].base_timber_factor = updatedBaseTimber;
            dbProducts[productIndex].shelf_tier_modifier_price = updatedShelfModifier;
            dbProducts[productIndex].base_hardware_count = updatedBaseHardware;
        }

        // 5. Force the active math configuration engine to run calculations with new parameters
        if (typeof executeCoreAnalyticalMathEngineRuns === "function") {
            executeCoreAnalyticalMathEngineRuns();
        }

        // 6. Trigger success notification
        if (typeof showToast === "function") {
            showToast("Product recipe saved to database!", 2500);
        } else {
            alert("Product recipe saved to database!");
        }

    } catch (err) {
        console.error("Database Save Failure:", err);
        if (typeof showToast === "function") {
            showToast("Failed to write modifications to database.", 4000);
        }
    }
}