const API_BASE = window.location.origin;
const UPDATE_INTERVAL = 1000; // Update every second to match metric pipeline

let inventoryData = {};
let chefsData = [];
let dlqRecords = [];
let dlqIndex = 0;

// Initialize dashboard
document.addEventListener('DOMContentLoaded', () => {
    updateDashboard();
    setInterval(updateDashboard, UPDATE_INTERVAL);
    
    // Set up generate orders button
    const generateBtn = document.getElementById('generate-orders-btn');
    if (generateBtn) {
        generateBtn.addEventListener('click', generateOrders);
    }
    
    // Set up restock button
    const restockBtn = document.getElementById('restock-btn');
    if (restockBtn) {
        restockBtn.addEventListener('click', restockInventory);
    }
});

async function generateOrders() {
    const btn = document.getElementById('generate-orders-btn');
    if (!btn) return;

    // Disable button and show loading state
    btn.disabled = true;
    const originalText = btn.textContent;
    btn.textContent = '⏳ Generating...';
    btn.classList.add('loading');

    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), 30000);

    try {
        const response = await fetch(`${API_BASE}/api/orders/generate`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({ count: 40 }),
            signal: controller.signal
        });

        clearTimeout(timeoutId);

        if (!response.ok && response.headers.get('content-type')?.indexOf('application/json') === -1) {
            throw new Error(`Server error: ${response.status}`);
        }

        const result = await response.json();

        if (result.status === 'success') {
            btn.textContent = `✅ Generated ${result.generated} orders!`;
            btn.classList.remove('loading');
            btn.classList.add('success');
        } else if (result.status === 'partial') {
            btn.textContent = `⚠️ ${result.generated}/${result.requested} orders (${result.failed} failed)`;
            btn.classList.remove('loading');
            btn.classList.add('error');
        } else {
            btn.textContent = `❌ Failed: ${result.error || 'Kafka unavailable'}`;
            btn.classList.remove('loading');
            btn.classList.add('error');
        }

        // Refresh dashboard immediately
        await updateDashboard();

        // Reset button after 3 seconds
        setTimeout(() => {
            btn.textContent = originalText;
            btn.classList.remove('success', 'error');
            btn.disabled = false;
        }, 3000);

    } catch (error) {
        clearTimeout(timeoutId);
        console.error('Error generating orders:', error);
        if (error.name === 'AbortError') {
            btn.textContent = '❌ Timed out - Kafka may be unreachable';
        } else {
            btn.textContent = '❌ Failed - Try Again';
        }
        btn.classList.remove('loading');
        btn.classList.add('error');

        // Reset button after 3 seconds
        setTimeout(() => {
            btn.textContent = originalText;
            btn.classList.remove('error');
            btn.disabled = false;
        }, 3000);
    }
}

async function addChef() {
    try {
        const response = await fetch(`${API_BASE}/api/scaling/add-chef`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' }
        });
        
        if (!response.ok) {
            const error = await response.json();
            console.error('Failed to add chef:', error.error);
            return;
        }
        
        await updateDashboard();
    } catch (error) {
        console.error('Error adding chef:', error);
    }
}

async function removeChef() {
    try {
        const response = await fetch(`${API_BASE}/api/scaling/remove-chef`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' }
        });
        
        if (!response.ok) {
            const error = await response.json();
            console.error('Failed to remove chef:', error.error);
            return;
        }
        
        await updateDashboard();
    } catch (error) {
        console.error('Error removing chef:', error);
    }
}

// Expose manual scaling functions to global scope for onclick handlers
window.addChef = addChef;
window.removeChef = removeChef;

async function restockInventory() {
    const btn = document.getElementById('restock-btn');
    if (!btn) return;
    
    // Disable button and show loading state
    btn.disabled = true;
    const originalText = btn.textContent;
    btn.textContent = '⏳ Restocking...';
    btn.classList.add('loading');
    
    try {
        const response = await fetch(`${API_BASE}/api/inventory/restock`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({ amount: 25 })
        });
        
        if (!response.ok) {
            throw new Error('Failed to restock inventory');
        }
        
        const result = await response.json();
        
        // Show success feedback
        btn.textContent = `✅ Restocked +${result.amountAdded} each!`;
        btn.classList.remove('loading');
        btn.classList.add('success');
        
        // Refresh dashboard immediately
        await updateDashboard();
        
        // Reset button after 2 seconds
        setTimeout(() => {
            btn.textContent = originalText;
            btn.classList.remove('success');
            btn.disabled = false;
        }, 2000);
        
    } catch (error) {
        console.error('Error restocking inventory:', error);
        btn.textContent = '❌ Failed - Try Again';
        btn.classList.remove('loading');
        btn.classList.add('error');
        
        // Reset button after 2 seconds
        setTimeout(() => {
            btn.textContent = originalText;
            btn.classList.remove('error');
            btn.disabled = false;
        }, 2000);
    }
}

async function updateDashboard() {
    try {
        await Promise.all([
            updateInventory(),
            updateChefs(),
            updateAutoScale(),
            updateDlq()
        ]);
        updateLastUpdateTime();
    } catch (error) {
        console.error('Error updating dashboard:', error);
        showError('Failed to update dashboard. Make sure the server is running.');
    }
}

async function updateInventory() {
    try {
        const response = await fetch(`${API_BASE}/api/inventory`);
        if (!response.ok) throw new Error('Failed to fetch inventory');
        
        inventoryData = await response.json();
        renderInventory();
    } catch (error) {
        console.error('Error fetching inventory:', error);
    }
}

async function updateChefs() {
    try {
        const response = await fetch(`${API_BASE}/api/chefs`);
        if (!response.ok) throw new Error('Failed to fetch chefs');
        
        chefsData = await response.json();
        renderChefs();
    } catch (error) {
        console.error('Error fetching chefs:', error);
    }
}

function renderInventory() {
    const container = document.getElementById('inventory-container');
    if (!container) return;

    if (Object.keys(inventoryData).length === 0) {
        container.innerHTML = '<div class="loading">No inventory data available</div>';
        return;
    }

    container.innerHTML = Object.entries(inventoryData)
        .map(([itemName, quantity]) => {
            const stockClass = getStockClass(quantity);
            const status = getStockStatus(quantity);
            return `
                <div class="inventory-item ${stockClass}">
                    <div class="item-name">${itemName}</div>
                    <div class="item-quantity">${quantity}</div>
                    <div class="item-status">${status}</div>
                </div>
            `;
        })
        .join('');
}

function renderChefs() {
    const container = document.getElementById('chefs-container');
    if (!container) return;

    if (chefsData.length === 0) {
        container.innerHTML = '<div class="loading">No chef data available. Start some chef consumers!</div>';
        return;
    }

    container.innerHTML = chefsData
        .map(chef => {
            const actionClass = getActionClass(chef.lastAction);
            const lastUpdate = formatTime(chef.lastUpdateTime);
            const deliveryCount = chef.lastOrderDeliveryCount || 0;
            return `
                <div class="chef-card">
                    <div class="chef-name">
                        👨‍🍳 ${chef.chefName}
                    </div>
                    <div class="chef-status">
                        <div class="status-label">Last Order:</div>
                        <div class="status-value">${chef.lastOrderId || 'None'}</div>
                    </div>
                    <div class="chef-status">
                        <div class="status-label">Last Action:</div>
                        <div class="status-value ${actionClass}">${formatAction(chef.lastAction)}</div>
                    </div>
                    <div class="chef-status">
                        <div class="status-label">Delivery Count:</div>
                        <div class="status-value">${deliveryCount > 0 ? deliveryCount : '-'}</div>
                    </div>
                    <div class="chef-stats">
                        <div class="stat-item">
                            <span class="stat-label">Last Update:</span>
                            <span class="stat-value">${lastUpdate}</span>
                        </div>
                    </div>
                </div>
            `;
        })
        .join('');
}

function getStockClass(quantity) {
    if (quantity < 10) return 'low-stock';
    if (quantity < 25) return 'medium-stock';
    return 'high-stock';
}

function getStockStatus(quantity) {
    if (quantity < 10) return '⚠️ Low Stock';
    if (quantity < 25) return '⚡ Medium Stock';
    return '✅ In Stock';
}

function getActionClass(action) {
    if (!action) return '';
    if (action.toUpperCase().includes('ACCEPT')) return 'status-accept';
    if (action.toUpperCase().includes('RELEASE')) return 'status-release';
    if (action.toUpperCase().includes('REJECT')) return 'status-reject';
    return '';
}

function formatAction(action) {
    if (!action) return 'None';
    return action.charAt(0).toUpperCase() + action.slice(1).toLowerCase();
}

function formatTime(timestamp) {
    if (!timestamp) return 'Never';
    const date = new Date(timestamp);
    const now = new Date();
    const diff = now - date;
    
    if (diff < 1000) return 'Just now';
    if (diff < 60000) return `${Math.floor(diff / 1000)}s ago`;
    if (diff < 3600000) return `${Math.floor(diff / 60000)}m ago`;
    return date.toLocaleTimeString();
}

function updateLastUpdateTime() {
    const element = document.getElementById('last-update');
    if (element) {
        element.textContent = new Date().toLocaleTimeString();
    }
}

async function updateAutoScale() {
    try {
        const response = await fetch(`${API_BASE}/api/autoscale`);
        if (!response.ok) throw new Error('Failed to fetch auto-scale status');
        
        const autoscaleData = await response.json();
        renderAutoScale(autoscaleData);
    } catch (error) {
        console.error('Error fetching auto-scale status:', error);
        // Show error in UI
        const container = document.getElementById('autoscale-container');
        if (container) {
            container.innerHTML = '<div class="loading">Error loading auto-scale status</div>';
        }
    }
}

function renderAutoScale(data) {
    const container = document.getElementById('autoscale-container');
    if (!container) {
        console.warn('autoscale-container not found in DOM');
        return;
    }

    if (data.enabled === false) {
        container.innerHTML = '<div class="loading">Auto-scaling is disabled</div>';
        return;
    }

    const queueDepth = data.queueDepth !== undefined ? data.queueDepth : 0;
    const activeChefs = data.activeChefs !== undefined ? data.activeChefs : 0;
    const targetChefs = data.targetChefs !== undefined ? data.targetChefs : 0;
    const rejectedOrders = data.rejectedOrders !== undefined ? data.rejectedOrders : 0;
    const chefNames = data.activeChefNames || [];
    const manualMode = data.manualMode === true;
    const maxChefs = data.maxChefs || 4;
    const minChefs = data.minChefs || 1;

    const kedaMode = data.kedaMode === true;
    const depthClass = queueDepth >= 4 ? 'high-queue' : queueDepth >= 2 ? 'medium-queue' : 'low-queue';
    const scalingStatus = activeChefs < targetChefs ? 'scaling-up' : activeChefs > targetChefs ? 'scaling-down' : 'stable';
    const rejectedClass = rejectedOrders > 0 ? 'rejected-orders' : '';

    // Manual scaling buttons (only shown in manual mode, not KEDA mode)
    const manualControls = manualMode && !kedaMode ? `
        <div class="manual-scaling-controls">
            <div class="manual-scaling-label">👆 Manual Scaling</div>
            <div class="manual-scaling-buttons">
                <button class="scale-btn remove-chef-btn" onclick="removeChef()" ${activeChefs <= minChefs ? 'disabled' : ''}>
                    ➖ Remove Chef
                </button>
                <button class="scale-btn add-chef-btn" onclick="addChef()" ${activeChefs >= maxChefs ? 'disabled' : ''}>
                    ➕ Add Chef
                </button>
            </div>
        </div>
    ` : '';

    const threshold = data.threshold || 4;

    // Target chefs card content depends on mode
    let targetLabel, targetValue, targetDescription;
    if (kedaMode) {
        targetLabel = 'Target Chefs';
        targetValue = targetChefs;
        targetDescription = `KEDA scaling`;
    } else if (manualMode) {
        targetLabel = 'Mode';
        targetValue = '👆';
        targetDescription = 'Manual scaling';
    } else {
        targetLabel = 'Target Chefs';
        targetValue = targetChefs;
        targetDescription = 'Desired count';
    }

    // Update the container with new data
    container.innerHTML = `
        <div class="autoscale-stats">
            <div class="stat-card ${depthClass}">
                <div class="stat-label">Queue Depth</div>
                <div class="stat-value">${queueDepth}</div>
                <div class="stat-description">Unprocessed messages</div>
            </div>
            <div class="stat-card ${manualMode ? '' : scalingStatus}">
                <div class="stat-label">${targetLabel}</div>
                <div class="stat-value">${targetValue}</div>
                <div class="stat-description">${targetDescription}</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Active Chefs</div>
                <div class="stat-value">${activeChefs}</div>
                <div class="stat-description">Currently running</div>
            </div>
            <div class="stat-card ${rejectedClass}">
                <div class="stat-label">Rejected Orders</div>
                <div class="stat-value">${rejectedOrders}</div>
                <div class="stat-description">Rejected</div>
            </div>
        </div>
        ${manualControls}
        ${chefNames.length > 0 ? `
            <div class="chef-names">
                <div class="chef-names-label">Active Chef Names:</div>
                <div class="chef-names-list">${chefNames.map(name => `<span class="chef-name-tag">${name}</span>`).join('')}</div>
            </div>
        ` : ''}
    `;
}

function escapeHtml(value) {
    if (value === null || value === undefined) return '';
    return String(value)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#39;');
}

async function updateDlq() {
    try {
        const [countResponse, recordsResponse] = await Promise.all([
            fetch(`${API_BASE}/api/dlq/count`),
            fetch(`${API_BASE}/api/dlq/records`)
        ]);
        if (!countResponse.ok) throw new Error('Failed to fetch DLQ count');
        if (!recordsResponse.ok) throw new Error('Failed to fetch DLQ records');

        const countData = await countResponse.json();
        const newRecords = await recordsResponse.json();

        // Keep the inspector pointed at the same record (by dlqOffset) across refreshes.
        const currentOffset = dlqRecords[dlqIndex] ? dlqRecords[dlqIndex].dlqOffset : undefined;
        dlqRecords = newRecords;
        if (dlqRecords.length === 0) {
            dlqIndex = 0;
        } else {
            const sameOffsetIndex = dlqRecords.findIndex(r => r.dlqOffset === currentOffset);
            dlqIndex = sameOffsetIndex >= 0 ? sameOffsetIndex : dlqRecords.length - 1;
        }

        renderDlqStats(countData);
        renderDlqInspector();
    } catch (error) {
        console.error('Error fetching DLQ data:', error);
        const container = document.getElementById('dlq-stats-container');
        if (container) {
            container.innerHTML = '<div class="loading">Error loading DLQ status</div>';
        }
    }
}

function renderDlqStats(countData) {
    const container = document.getElementById('dlq-stats-container');
    if (!container) return;

    const dlqCount = countData.count !== undefined ? countData.count : 0;
    const rejectedCount = countData.rejected !== undefined ? countData.rejected : 0;
    const dlqClass = dlqCount > 0 ? 'rejected-orders' : '';

    container.innerHTML = `
        <div class="stat-card ${dlqClass}">
            <div class="stat-label">DLQ Records</div>
            <div class="stat-value">${dlqCount}</div>
            <div class="stat-description">Total landed in the dead-letter queue</div>
        </div>
        <div class="stat-card">
            <div class="stat-label">Rejected Orders</div>
            <div class="stat-value">${rejectedCount}</div>
            <div class="stat-description">Explicit Reject acks only</div>
        </div>
    `;
}

function renderDlqInspector() {
    const container = document.getElementById('dlq-inspector-container');
    if (!container) return;

    if (dlqRecords.length === 0) {
        container.innerHTML = '<div class="loading">No DLQ records yet. Configure a DLQ topic on chefs-share-group and reject or exhaust some orders to see them here.</div>';
        return;
    }

    const record = dlqRecords[dlqIndex];
    const orderLabel = escapeHtml(record.orderId || '(unparsed)');
    const payloadHtml = record.hasPayload
        ? `<div class="status-value status-accept">Order ${orderLabel} — ${record.orderItemCount || 0} item(s) copied</div>`
        : `<div class="status-value status-reject">None — group has errors.deadletterqueue.copy.record.enable=false (headers only)</div>`;

    container.innerHTML = `
        <div class="dlq-nav">
            <button class="scale-btn remove-chef-btn" onclick="dlqPrev()" ${dlqIndex <= 0 ? 'disabled' : ''}>⬅ Prev</button>
            <div class="dlq-nav-position">Record ${dlqIndex + 1} of ${dlqRecords.length}</div>
            <button class="scale-btn add-chef-btn" onclick="dlqNext()" ${dlqIndex >= dlqRecords.length - 1 ? 'disabled' : ''}>Next ➡</button>
        </div>
        <div class="dlq-record-detail">
            <div class="chef-status">
                <div class="status-label">Original Topic / Partition / Offset:</div>
                <div class="status-value">${escapeHtml(record.originalTopic) || '-'} / ${escapeHtml(record.originalPartition) || '-'} / ${escapeHtml(record.originalOffset) || '-'}</div>
            </div>
            <div class="chef-status">
                <div class="status-label">Group:</div>
                <div class="status-value">${escapeHtml(record.group) || '-'}</div>
            </div>
            <div class="chef-status">
                <div class="status-label">Delivery Count:</div>
                <div class="status-value">${escapeHtml(record.deliveryCount) || '-'}</div>
            </div>
            <div class="chef-status">
                <div class="status-label">Reason:</div>
                <div class="status-value">${escapeHtml(record.reason) || '-'}</div>
            </div>
            <div class="chef-status">
                <div class="status-label">Payload:</div>
                ${payloadHtml}
            </div>
        </div>
    `;
}

function dlqPrev() {
    if (dlqIndex > 0) {
        dlqIndex--;
        renderDlqInspector();
    }
}

function dlqNext() {
    if (dlqIndex < dlqRecords.length - 1) {
        dlqIndex++;
        renderDlqInspector();
    }
}

window.dlqPrev = dlqPrev;
window.dlqNext = dlqNext;

function showError(message) {
    const container = document.querySelector('.container');
    if (container) {
        let errorDiv = document.querySelector('.error');
        if (!errorDiv) {
            errorDiv = document.createElement('div');
            errorDiv.className = 'error';
            container.insertBefore(errorDiv, container.firstChild);
        }
        errorDiv.textContent = message;
    }
}

