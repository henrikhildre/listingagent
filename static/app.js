/**
 * ListingAgent — Frontend Application
 *
 * State-machine driven single-page app.
 * States: UPLOAD -> DISCOVER -> INTERVIEW -> RECIPE_TEST -> EXECUTING -> RESULTS
 *
 * No framework. Vanilla JS. Chat is the primary interface for phases 1-3.
 *
 * Table of Contents:
 *   Global State .................. ~14
 *   Utility Functions ............. ~30
 *   View Management ............... ~227
 *   Upload (Phase 0) .............. ~291
 *   Chat (Phases 1-3) ............. ~654
 *   Build Data Model .............. ~988
 *   Interview (Phase 2) ........... ~1055
 *   Recipe Building & Testing ..... ~1100
 *   Execution (Phase 4) ........... ~1364
 *   Context Panel ................. ~1798
 *   Download & Export .............. ~2094
 *   Listing Detail Modal .......... ~2160
 *   Login ......................... ~2343
 *   Initialization ................ ~2385
 */

// ============================================================================
// Global State
// ============================================================================

const state = {
    phase: 'UPLOAD',
    jobId: null,
    username: null,
    conversationHistory: [],
    dataModel: null,
    styleProfile: null,
    recipe: null,
    testResults: [],
    batchResults: [],
    fullListings: [],
    ws: null,
    uploadedFiles: [],
    executionStats: null,
};

// ============================================================================
// Utility Functions
// ============================================================================

/**
 * Wrapper around fetch with JSON handling and error display.
 */
async function api(endpoint, options = {}) {
    const defaults = {
        headers: {},
    };

    // Only set Content-Type for non-FormData bodies
    if (options.body && !(options.body instanceof FormData)) {
        defaults.headers['Content-Type'] = 'application/json';
    }

    const config = {
        ...defaults,
        ...options,
        headers: { ...defaults.headers, ...options.headers },
    };

    try {
        const response = await fetch(endpoint, config);

        if (response.status === 401) {
            showView('login-view');
            throw new Error('Session expired. Please log in again.');
        }

        if (!response.ok) {
            let errorMessage = `Request failed (${response.status})`;
            try {
                const errorData = await response.json();
                errorMessage = errorData.detail || errorData.message || errorMessage;
            } catch (_) {
                // Response body was not JSON
            }
            throw new Error(errorMessage);
        }

        // Check if response has content
        const contentType = response.headers.get('content-type');
        if (contentType && contentType.includes('application/json')) {
            return await response.json();
        }

        return response;
    } catch (error) {
        if (error.name === 'TypeError' && error.message === 'Failed to fetch') {
            showToast('Network error. Is the server running?', 'error');
        }
        throw error;
    }
}

/**
 * Show a toast notification.
 */
function showToast(message, type = 'info') {
    // Remove any existing toast
    const existing = document.querySelector('.toast');
    if (existing) existing.remove();

    const toast = document.createElement('div');
    toast.className = `toast ${type === 'error' ? 'toast-error' : type === 'success' ? 'toast-success' : ''}`;
    toast.textContent = message;
    document.body.appendChild(toast);

    setTimeout(() => {
        toast.style.opacity = '0';
        toast.style.transition = 'opacity 300ms';
        setTimeout(() => toast.remove(), 300);
    }, 4000);
}

/**
 * Show loading overlay with a message.
 */
function showLoading(message = 'Processing...') {
    let overlay = document.getElementById('loading-overlay');
    if (!overlay) {
        overlay = document.createElement('div');
        overlay.id = 'loading-overlay';
        overlay.className = 'fixed inset-0 bg-black/30 flex items-center justify-center z-50';
        overlay.innerHTML = `
            <div class="bg-white rounded-xl p-8 shadow-xl flex flex-col items-center gap-4 max-w-sm mx-4">
                <div class="spinner spinner-lg"></div>
                <p id="loading-message" class="text-slate-600 text-center"></p>
            </div>
        `;
        document.body.appendChild(overlay);
    }

    const msg = document.getElementById('loading-text') || document.getElementById('loading-message');
    if (msg) msg.textContent = message;
    overlay.classList.remove('hidden');
}

/**
 * Hide loading overlay.
 */
function hideLoading() {
    const overlay = document.getElementById('loading-overlay');
    if (overlay) {
        overlay.classList.add('hidden');
    }
}

/**
 * Show an inline progress indicator in the chat (spinner + status text).
 * Returns the wrapper element for later updates/removal.
 */
function showInlineProgress(text) {
    const chatMessages = document.getElementById('chat-messages');
    if (!chatMessages) return null;

    const wrapper = document.createElement('div');
    wrapper.className = 'inline-progress';

    const bubble = document.createElement('div');
    bubble.className = 'inline-progress-bubble';
    bubble.innerHTML = `
        <div class="spinner spinner-sm"></div>
        <span class="inline-progress-text">${escapeHtml(text)}</span>
    `;

    wrapper.appendChild(bubble);
    chatMessages.appendChild(wrapper);
    chatMessages.scrollTop = chatMessages.scrollHeight;

    return wrapper;
}

/**
 * Update the text of an inline progress indicator.
 */
function updateInlineProgress(el, text) {
    if (!el) return;
    const span = el.querySelector('.inline-progress-text');
    if (span) span.textContent = text;

    const chatMessages = document.getElementById('chat-messages');
    if (chatMessages) chatMessages.scrollTop = chatMessages.scrollHeight;
}

/**
 * Remove an inline progress indicator from the chat.
 */
function removeInlineProgress(el) {
    if (el && el.parentNode) {
        el.parentNode.removeChild(el);
    }
}

/**
 * Format file size in human-readable form.
 */
function formatFileSize(bytes) {
    if (bytes < 1024) return bytes + ' B';
    if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + ' KB';
    return (bytes / (1024 * 1024)).toFixed(1) + ' MB';
}

/**
 * Get a file type icon class based on extension.
 */
function getFileIcon(filename) {
    const ext = filename.split('.').pop().toLowerCase();
    const imageExts = ['jpg', 'jpeg', 'png', 'webp', 'gif'];
    const sheetExts = ['xlsx', 'xls', 'csv', 'tsv'];
    const docExts = ['pdf', 'doc', 'docx'];

    if (imageExts.includes(ext)) return 'image';
    if (sheetExts.includes(ext)) return 'spreadsheet';
    if (docExts.includes(ext)) return 'document';
    return 'other';
}

/**
 * Simple markdown-ish formatting for chat messages.
 * Handles **bold**, `code`, ```code blocks```, and newlines.
 */
function renderJsonCard(obj) {
    const formatLabel = (key) => key.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase());
    const pill = (text) => `<span style="display:inline-block;background:#EEF2FF;color:#4F46E5;font-size:12px;padding:3px 10px;border-radius:999px;margin:2px 4px 2px 0">${escapeHtml(String(text))}</span>`;
    let rows = '';
    for (const [key, value] of Object.entries(obj)) {
        if (value === null || value === undefined || value === '') continue;
        if (Array.isArray(value)) {
            const items = value.filter(v => v != null && v !== '');
            if (!items.length) continue;
            rows += `<div style="padding:6px 0">
                <div style="color:#64748B;font-size:11px;text-transform:uppercase;letter-spacing:0.05em;margin-bottom:3px">${escapeHtml(formatLabel(key))}</div>
                <div>${items.map(v => typeof v === 'object' ? `<code style="font-size:12px">${escapeHtml(JSON.stringify(v))}</code>` : pill(v)).join('')}</div>
            </div>`;
        } else if (typeof value === 'object') {
            rows += `<div style="padding:6px 0">
                <div style="color:#64748B;font-size:11px;text-transform:uppercase;letter-spacing:0.05em;margin-bottom:1px">${escapeHtml(formatLabel(key))}</div>
                <div style="color:#1E293B;font-size:13px"><code>${escapeHtml(JSON.stringify(value))}</code></div>
            </div>`;
        } else {
            rows += `<div style="padding:6px 0">
                <div style="color:#64748B;font-size:11px;text-transform:uppercase;letter-spacing:0.05em;margin-bottom:1px">${escapeHtml(formatLabel(key))}</div>
                <div style="color:#1E293B;font-size:13px">${escapeHtml(String(value))}</div>
            </div>`;
        }
    }
    return `<div style="display:grid;grid-template-columns:1fr 1fr;gap:0 20px">${rows}</div>`;
}

function formatMessage(text) {
    if (!text) return '';
    if (typeof marked !== 'undefined') {
        const renderer = new marked.Renderer();
        const defaultCode = renderer.code.bind(renderer);
        renderer.code = function({ text: code, lang }) {
            const src = (typeof code === 'string' ? code : code?.text || '').trim();
            if (lang === 'json' || (!lang && src.startsWith('{'))) {
                try {
                    const obj = JSON.parse(src);
                    if (obj && typeof obj === 'object' && !Array.isArray(obj)) {
                        return renderJsonCard(obj);
                    }
                } catch {}
            }
            return defaultCode({ text: code, lang });
        };
        return marked.parse(text, { renderer });
    }
    // Fallback: escape HTML and add line breaks
    return text
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/\n/g, '<br>');
}

// ============================================================================
// View Management
// ============================================================================

/**
 * Switch the active view. Hides all views, shows the target.
 */
function showView(viewName) {
    const views = ['login-view', 'upload-view', 'chat-view', 'execution-view'];

    views.forEach(id => {
        const el = document.getElementById(id);
        if (el) {
            el.classList.add('hidden');
            el.classList.remove('view-enter');
        }
    });

    const target = document.getElementById(viewName);
    if (target) {
        target.classList.remove('hidden');
        target.classList.add('view-enter');
    }
}

/**
 * Update the phase badge in the chat view header.
 */
function updatePhaseIndicator(phase) {
    const container = document.getElementById('phase-indicator');
    if (!container) return;

    const phaseOrder = ['discovery', 'interview', 'recipe'];
    const phaseMap = {
        'DISCOVER': 'discovery',
        'INTERVIEW': 'interview',
        'RECIPE_TEST': 'recipe',
        'EXECUTING': 'recipe',
        'RESULTS': 'recipe',
    };

    const activePhase = phaseMap[phase] || 'discovery';
    const activeIndex = phaseOrder.indexOf(activePhase);

    phaseOrder.forEach((p, i) => {
        const span = container.querySelector(`[data-phase="${p}"]`);
        if (!span) return;

        // Reset classes
        span.className = 'px-3 py-1 rounded-full';

        if (i < activeIndex) {
            // Completed
            span.classList.add('bg-green-100', 'text-green-600');
        } else if (i === activeIndex) {
            // Active
            span.classList.add('bg-blue-100', 'text-blue-600');
        } else {
            // Upcoming
            span.classList.add('bg-slate-100', 'text-slate-400');
        }
    });
}

// ============================================================================
// Upload (Phase 0)
// ============================================================================

/**
 * Initialize drag-and-drop, file input, and paste input handlers.
 */
function initUpload() {
    const dropZone = document.getElementById('drop-zone');
    const fileInput = document.getElementById('file-input');

    if (!dropZone || !fileInput) return;

    // Click drop zone to trigger file input
    dropZone.addEventListener('click', () => fileInput.click());

    // Drag events
    dropZone.addEventListener('dragenter', (e) => {
        e.preventDefault();
        dropZone.classList.add('dragover');
    });

    dropZone.addEventListener('dragover', (e) => {
        e.preventDefault();
        dropZone.classList.add('dragover');
    });

    dropZone.addEventListener('dragleave', (e) => {
        e.preventDefault();
        dropZone.classList.remove('dragover');
    });

    dropZone.addEventListener('drop', (e) => {
        e.preventDefault();
        dropZone.classList.remove('dragover');
        if (e.dataTransfer.files.length > 0) {
            handleFiles(e.dataTransfer.files);
        }
    });

    // File input change
    fileInput.addEventListener('change', () => {
        if (fileInput.files.length > 0) {
            handleFiles(fileInput.files);
        }
    });

    // Paste input handlers
    const pasteInput = document.getElementById('paste-input');
    const pasteExploreBtn = document.getElementById('paste-explore-btn');
    const pasteCharCount = document.getElementById('paste-char-count');

    if (pasteInput) {
        pasteInput.addEventListener('input', () => {
            const len = pasteInput.value.length;
            if (pasteCharCount) {
                pasteCharCount.textContent = `${len.toLocaleString()} characters`;
                pasteCharCount.classList.toggle('text-red-500', len > 200000);
                pasteCharCount.classList.toggle('text-slate-400', len <= 200000);
            }
            if (pasteExploreBtn) {
                pasteExploreBtn.disabled = len === 0 || len > 200000;
            }
        });
    }
}

/**
 * Switch between upload and paste input modes.
 */
function switchInputMode(mode) {
    const uploadMode = document.getElementById('upload-mode');
    const pasteMode = document.getElementById('paste-mode');
    const tabUpload = document.getElementById('tab-upload');
    const tabPaste = document.getElementById('tab-paste');

    if (!uploadMode || !pasteMode) return;

    if (mode === 'paste') {
        uploadMode.classList.add('hidden');
        pasteMode.classList.remove('hidden');
        tabUpload.classList.remove('input-tab-active');
        tabUpload.classList.add('text-slate-500');
        tabPaste.classList.add('input-tab-active');
        tabPaste.classList.remove('text-slate-500');
        // Focus the textarea
        const pasteInput = document.getElementById('paste-input');
        if (pasteInput) pasteInput.focus();
    } else {
        uploadMode.classList.remove('hidden');
        pasteMode.classList.add('hidden');
        tabUpload.classList.add('input-tab-active');
        tabUpload.classList.remove('text-slate-500');
        tabPaste.classList.remove('input-tab-active');
        tabPaste.classList.add('text-slate-500');
    }
}

/**
 * Display uploaded files in the file list UI.
 */
function handleFiles(fileList) {
    const newFiles = Array.from(fileList);
    state.uploadedFiles = [...state.uploadedFiles, ...newFiles];

    renderFileList();

    // Enable the explore button
    const exploreBtn = document.getElementById('explore-btn');
    if (exploreBtn) {
        exploreBtn.disabled = false;
    }
}

/**
 * Render the file list in the upload view.
 */
function renderFileList() {
    const container = document.getElementById('file-list');
    if (!container) return;

    if (state.uploadedFiles.length === 0) {
        container.innerHTML = '<p class="text-slate-400 text-sm text-center py-4">No files selected</p>';
        return;
    }

    const iconMap = {
        image: `<svg class="file-item-icon" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 16l4.586-4.586a2 2 0 012.828 0L16 16m-2-2l1.586-1.586a2 2 0 012.828 0L20 14m-6-6h.01M6 20h12a2 2 0 002-2V6a2 2 0 00-2-2H6a2 2 0 00-2 2v12a2 2 0 002 2z" /></svg>`,
        spreadsheet: `<svg class="file-item-icon" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 17v-2m3 2v-4m3 4v-6m2 10H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" /></svg>`,
        document: `<svg class="file-item-icon" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M7 21h10a2 2 0 002-2V9.414a1 1 0 00-.293-.707l-5.414-5.414A1 1 0 0012.586 3H7a2 2 0 00-2 2v14a2 2 0 002 2z" /></svg>`,
        other: `<svg class="file-item-icon" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" /></svg>`,
    };

    container.innerHTML = state.uploadedFiles.map((file, i) => {
        const type = getFileIcon(file.name);
        return `
            <div class="file-item">
                <div class="flex items-center min-w-0">
                    ${iconMap[type] || iconMap.other}
                    <span class="file-item-name">${file.name}</span>
                </div>
                <span class="file-item-size">${formatFileSize(file.size)}</span>
            </div>
        `;
    }).join('');
}

/**
 * Upload files to the server and start discovery.
 */
async function uploadFiles() {
    if (state.uploadedFiles.length === 0) {
        showToast('Please add some files first.', 'error');
        return;
    }

    showLoading('Uploading files...');

    try {
        const formData = new FormData();
        state.uploadedFiles.forEach(file => {
            formData.append('files', file);
        });

        const result = await api('/api/upload', {
            method: 'POST',
            body: formData,
        });

        state.jobId = result.job_id;
        hideLoading();
        showToast(`${state.uploadedFiles.length} files uploaded successfully.`, 'success');

        // Stash cache info for after preview
        if (result.cache_hit && result.cache_meta) {
            state._pendingCache = { meta: result.cache_meta, fingerprint: result.fingerprint };
        }

        // Show data preview before starting discovery
        await showDataPreview();
    } catch (error) {
        hideLoading();
        showToast('Upload failed: ' + error.message, 'error');
    }
}

/**
 * Submit pasted text and start discovery.
 */
async function pasteAndExplore() {
    const pasteInput = document.getElementById('paste-input');
    if (!pasteInput) return;

    const text = pasteInput.value.trim();
    if (!text) {
        showToast('Please paste some text first.', 'error');
        return;
    }

    if (text.length > 200000) {
        showToast('Text exceeds 200,000 character limit.', 'error');
        return;
    }

    showLoading('Processing pasted text...');

    try {
        const result = await api('/api/paste', {
            method: 'POST',
            body: JSON.stringify({ text }),
        });

        state.jobId = result.job_id;
        hideLoading();
        showToast(`Text received (${result.text_length.toLocaleString()} characters).`, 'success');

        // Show data preview before starting discovery
        await showDataPreview();
    } catch (error) {
        hideLoading();
        showToast('Failed to submit text: ' + error.message, 'error');
    }
}

/**
 * Open the demo picker modal and fetch the catalog.
 */
async function openDemoPicker() {
    const overlay = document.getElementById('demo-picker-overlay');
    const grid = document.getElementById('demo-picker-grid');
    overlay.classList.remove('hidden');
    grid.innerHTML = '<div class="col-span-full flex justify-center py-8"><div class="spinner"></div></div>';

    try {
        const data = await api('/api/demo-catalog');
        renderDemoGrid(data.demos, grid);
    } catch (err) {
        grid.innerHTML = `<p class="col-span-full text-center text-red-500 text-sm py-8">Failed to load demos: ${err.message}</p>`;
    }
}

function closeDemoPicker() {
    document.getElementById('demo-picker-overlay').classList.add('hidden');
}

const DEMO_ICONS = {
    table: `<svg class="w-6 h-6" fill="none" stroke="currentColor" stroke-width="1.5" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" d="M3.375 19.5h17.25m-17.25 0a1.125 1.125 0 01-1.125-1.125M3.375 19.5h7.5c.621 0 1.125-.504 1.125-1.125m-9.75 0V5.625m0 12.75v-1.5c0-.621.504-1.125 1.125-1.125m18.375 2.625V5.625m0 12.75c0 .621-.504 1.125-1.125 1.125m1.125-1.125v-1.5c0-.621-.504-1.125-1.125-1.125m0 3.75h-7.5A1.125 1.125 0 0112 18.375m9.75-12.75c0-.621-.504-1.125-1.125-1.125H3.375c-.621 0-1.125.504-1.125 1.125m19.5 0v1.5c0 .621-.504 1.125-1.125 1.125M2.25 5.625v1.5c0 .621.504 1.125 1.125 1.125m0 0h17.25m-17.25 0h7.5c.621 0 1.125.504 1.125 1.125M3.375 8.25c-.621 0-1.125.504-1.125 1.125v1.5c0 .621.504 1.125 1.125 1.125m17.25-3.75h-7.5c-.621 0-1.125.504-1.125 1.125m8.625-1.125c.621 0 1.125.504 1.125 1.125v1.5c0 .621-.504 1.125-1.125 1.125m-17.25 0h7.5m-7.5 0c-.621 0-1.125.504-1.125 1.125v1.5c0 .621.504 1.125 1.125 1.125M12 10.875v-1.5m0 1.5c0 .621-.504 1.125-1.125 1.125M12 10.875c0 .621.504 1.125 1.125 1.125m-2.25 0c.621 0 1.125.504 1.125 1.125M10.875 12c-.621 0-1.125.504-1.125 1.125M12 12c.621 0 1.125.504 1.125 1.125m-2.25 0c.621 0 1.125.504 1.125 1.125m0 0v1.5c0 .621-.504 1.125-1.125 1.125M12 15.375c0-.621.504-1.125 1.125-1.125"/></svg>`,
    clipboard: `<svg class="w-6 h-6" fill="none" stroke="currentColor" stroke-width="1.5" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" d="M15.666 3.888A2.25 2.25 0 0013.5 2.25h-3c-1.03 0-1.9.693-2.166 1.638m7.332 0c.055.194.084.4.084.612v0a.75.75 0 01-.75.75H9.75a.75.75 0 01-.75-.75v0c0-.212.03-.418.084-.612m7.332 0c.646.049 1.288.11 1.927.184 1.1.128 1.907 1.077 1.907 2.185V19.5a2.25 2.25 0 01-2.25 2.25H6.75A2.25 2.25 0 014.5 19.5V6.257c0-1.108.806-2.057 1.907-2.185a48.208 48.208 0 011.927-.184"/></svg>`,
    code: `<svg class="w-6 h-6" fill="none" stroke="currentColor" stroke-width="1.5" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" d="M17.25 6.75L22.5 12l-5.25 5.25m-10.5 0L1.5 12l5.25-5.25m7.5-3l-4.5 16.5"/></svg>`,
    photo: `<svg class="w-6 h-6" fill="none" stroke="currentColor" stroke-width="1.5" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" d="M2.25 15.75l5.159-5.159a2.25 2.25 0 013.182 0l5.159 5.159m-1.5-1.5l1.409-1.409a2.25 2.25 0 013.182 0l2.909 2.909M2.25 18V6a2.25 2.25 0 012.25-2.25h15A2.25 2.25 0 0121.75 6v12A2.25 2.25 0 0119.5 20.25h-15A2.25 2.25 0 012.25 18z"/></svg>`,
};

function renderDemoGrid(demos, container) {
    container.innerHTML = '';
    for (const demo of demos) {
        const card = document.createElement('button');
        card.className = 'demo-card';
        card.onclick = () => confirmDemoSelection(demo.id, demo.title);

        // Build preview section
        let previewHtml = '';
        if (demo.preview_images && demo.preview_images.length) {
            const imgs = demo.preview_images.slice(0, 3).map(f =>
                `<img src="/api/demo-image/${demo.id}/${f}" alt="" class="demo-card-thumb">`
            ).join('');
            previewHtml = `<div class="demo-card-thumbs">${imgs}</div>`;
        } else if (demo.preview_text) {
            const escaped = demo.preview_text.slice(0, 120).replace(/</g, '&lt;').replace(/>/g, '&gt;');
            previewHtml = `<div class="demo-card-text-preview">${escaped}...</div>`;
        }

        card.innerHTML = `
            <div class="demo-card-header">
                <div class="demo-card-icon">${DEMO_ICONS[demo.icon] || DEMO_ICONS.table}</div>
                <span class="demo-card-tag">${demo.tag}</span>
            </div>
            <div class="demo-card-title">${demo.title}</div>
            <div class="demo-card-desc">${demo.description}</div>
            ${previewHtml}
        `;
        container.appendChild(card);
    }
}

async function confirmDemoSelection(demoId, demoTitle) {
    closeDemoPicker();
    showLoading(`Loading ${demoTitle}...`);

    try {
        const result = await api('/api/load-demo', {
            method: 'POST',
            body: JSON.stringify({ demo_id: demoId }),
        });

        state.jobId = result.job_id;
        hideLoading();
        showToast(`Demo loaded: ${result.file_count} files.`, 'success');

        // Stash cache info for after preview
        if (result.cache_hit && result.cache_meta) {
            state._pendingCache = { meta: result.cache_meta, fingerprint: result.fingerprint };
        }

        await showDataPreview();
    } catch (error) {
        hideLoading();
        showToast('Failed to load demo: ' + error.message, 'error');
    }
}

/**
 * Show a modal dialog when a pipeline cache hit is detected.
 */
function showCacheHitDialog(meta, fingerprint) {
    const platform = meta.platform ? ` on ${meta.platform}` : '';
    const productCount = meta.product_count || '?';

    const overlay = document.createElement('div');
    overlay.id = 'cache-hit-overlay';
    overlay.className = 'fixed inset-0 z-50 flex items-center justify-center bg-black/40 backdrop-blur-sm';
    overlay.innerHTML = `
        <div class="bg-white rounded-2xl shadow-2xl w-full max-w-md mx-4 overflow-hidden" style="animation: fadeInUp var(--transition-medium)">
            <div class="px-6 py-5 border-b border-slate-200">
                <h2 class="text-lg font-bold text-slate-800">Recognized format</h2>
                <p class="text-sm text-slate-500 mt-1">You've processed data with these columns before${platform}.</p>
            </div>
            <div class="p-6 space-y-3">
                <button onclick="applyCacheChoice('full_reuse', '${fingerprint}')" class="w-full text-left px-4 py-3 rounded-xl border border-slate-200 hover:border-blue-300 hover:bg-blue-50 transition group">
                    <div class="text-sm font-semibold text-slate-800 group-hover:text-blue-700">Use previous recipe</div>
                    <div class="text-xs text-slate-500 mt-0.5">Skip straight to generating listings — same style &amp; recipe as last time.</div>
                </button>
                <button onclick="applyCacheChoice('adjust_style', '${fingerprint}')" class="w-full text-left px-4 py-3 rounded-xl border border-slate-200 hover:border-blue-300 hover:bg-blue-50 transition group">
                    <div class="text-sm font-semibold text-slate-800 group-hover:text-blue-700">Adjust style / recipe</div>
                    <div class="text-xs text-slate-500 mt-0.5">Reuse data mapping but redo the brand interview and recipe.</div>
                </button>
                <button onclick="applyCacheChoice('fresh', '${fingerprint}')" class="w-full text-left px-4 py-3 rounded-xl border border-slate-200 hover:border-slate-300 hover:bg-slate-50 transition group">
                    <div class="text-sm font-semibold text-slate-800">Start fresh</div>
                    <div class="text-xs text-slate-500 mt-0.5">Ignore the cache and run the full pipeline from scratch.</div>
                </button>
            </div>
        </div>
    `;
    document.body.appendChild(overlay);
}

/**
 * Handle the user's choice from the cache hit dialog.
 */
async function applyCacheChoice(mode, fingerprint) {
    // Remove dialog
    const overlay = document.getElementById('cache-hit-overlay');
    if (overlay) overlay.remove();

    if (mode === 'fresh') {
        await startDiscovery();
        return;
    }

    showLoading('Applying saved pipeline...');
    try {
        await api('/api/apply-cache', {
            method: 'POST',
            body: JSON.stringify({ job_id: state.jobId, fingerprint, mode }),
        });
        hideLoading();
        await startDiscovery();
    } catch (error) {
        hideLoading();
        showToast('Failed to apply cache: ' + error.message, 'error');
        await startDiscovery();
    }
}

/**
 * Clear all previous session state so a new upload starts fresh.
 */
function resetSession() {
    state.conversationHistory = [];
    state.dataModel = null;
    state.styleProfile = null;
    state.recipe = null;
    state.testResults = [];
    state.batchResults = [];
    state.fullListings = [];
    state.executionStats = null;
    if (state.ws) {
        state.ws.close();
        state.ws = null;
    }
    const chatMessages = document.getElementById('chat-messages');
    if (chatMessages) chatMessages.innerHTML = '';
    resetExecutionView();
}

/**
 * Prompt user to confirm starting a new job, then reset everything.
 */
function confirmNewJob() {
    // If on upload view with no job, nothing to lose
    if (state.phase === 'UPLOAD' && !state.jobId) return;

    if (!confirm('Start a new job? Current progress will be lost.')) return;

    resetSession();
    state.jobId = null;
    state.phase = 'UPLOAD';
    state.uploadedFiles = [];

    // Reset upload UI elements
    const fileList = document.getElementById('file-list');
    if (fileList) fileList.innerHTML = '';
    const exploreBtn = document.getElementById('explore-btn');
    if (exploreBtn) exploreBtn.disabled = true;
    const pasteInput = document.getElementById('paste-input');
    if (pasteInput) pasteInput.value = '';
    const pasteCharCount = document.getElementById('paste-char-count');
    if (pasteCharCount) pasteCharCount.textContent = '0 characters';
    const pasteExploreBtn = document.getElementById('paste-explore-btn');
    if (pasteExploreBtn) pasteExploreBtn.disabled = true;
    const fileInput = document.getElementById('file-input');
    if (fileInput) fileInput.value = '';

    showView('upload-view');
}

/**
 * Fetch and display a data preview so the user can inspect input before processing.
 */
async function showDataPreview() {
    try {
        const data = await api(`/api/preview-data/${state.jobId}`);
        const preview = data.preview;

        let html = '<div class="data-preview-content">';

        // Spreadsheet previews
        if (preview.spreadsheet_previews) {
            for (const [fname, sp] of Object.entries(preview.spreadsheet_previews)) {
                if (sp.error) continue;
                html += `<div class="preview-section">
                    <div class="preview-section-title">${escapeHtml(fname)} <span class="preview-badge">${sp.total_rows} rows</span></div>
                    <div class="preview-table-wrap">
                        <table class="preview-table"><thead><tr>`;
                for (const h of sp.headers || []) {
                    html += `<th>${escapeHtml(h)}</th>`;
                }
                html += `</tr></thead><tbody>`;
                for (const row of sp.rows || []) {
                    html += '<tr>';
                    for (const cell of row) {
                        html += `<td>${escapeHtml(cell)}</td>`;
                    }
                    html += '</tr>';
                }
                html += `</tbody></table></div></div>`;
            }
        }

        // JSON previews
        if (preview.json_previews) {
            for (const [fname, jp] of Object.entries(preview.json_previews)) {
                if (jp.error) continue;
                html += `<div class="preview-section">
                    <div class="preview-section-title">${escapeHtml(fname)} <span class="preview-badge">${jp.total_rows} items</span></div>
                    <div class="preview-table-wrap">
                        <table class="preview-table"><thead><tr>`;
                for (const h of jp.headers || []) {
                    html += `<th>${escapeHtml(h)}</th>`;
                }
                html += `</tr></thead><tbody>`;
                for (const row of jp.rows || []) {
                    html += '<tr>';
                    for (const cell of row) {
                        html += `<td>${escapeHtml(cell)}</td>`;
                    }
                    html += '</tr>';
                }
                html += `</tbody></table></div></div>`;
            }
        }

        // Pasted text
        if (preview.pasted_text) {
            const truncated = preview.pasted_text.length > 2000
                ? preview.pasted_text.slice(0, 2000) + '...'
                : preview.pasted_text;
            html += `<div class="preview-section">
                <div class="preview-section-title">Pasted Text <span class="preview-badge">${preview.pasted_text.length.toLocaleString()} chars</span></div>
                <pre class="preview-text">${escapeHtml(truncated)}</pre>
            </div>`;
        }

        // Image thumbnails
        if (preview.images && preview.images.length > 0) {
            html += `<div class="preview-section">
                <div class="preview-section-title">Images <span class="preview-badge">${preview.images.length}</span></div>
                <div class="preview-image-grid">`;
            for (const img of preview.images) {
                html += `<div class="preview-thumb">
                    <img src="/api/job-image/${state.jobId}/${encodeURIComponent(img)}" alt="${escapeHtml(img)}" loading="lazy">
                    <div class="preview-thumb-name">${escapeHtml(img)}</div>
                </div>`;
            }
            html += `</div></div>`;
        }

        html += '</div>';

        // Build the overlay
        const overlay = document.createElement('div');
        overlay.id = 'data-preview-overlay';
        overlay.className = 'fixed inset-0 z-50 flex items-center justify-center bg-black/40 backdrop-blur-sm';
        overlay.innerHTML = `
            <div class="bg-white rounded-2xl shadow-2xl w-full max-w-3xl mx-4 max-h-[85vh] flex flex-col" style="animation: fadeInUp var(--transition-medium)">
                <div class="px-6 py-4 border-b border-slate-200 flex items-center justify-between shrink-0">
                    <div>
                        <h2 class="text-lg font-bold text-slate-800">Data Preview</h2>
                        <p class="text-sm text-slate-500 mt-0.5">${preview.summary || 'Review your input data before processing.'}</p>
                    </div>
                </div>
                <div class="flex-1 overflow-y-auto p-6">
                    ${html}
                </div>
                <div class="px-6 py-4 border-t border-slate-200 flex justify-end gap-3 shrink-0">
                    <button onclick="closeDataPreview(false)" class="px-4 py-2 text-sm font-medium text-slate-600 hover:text-slate-800 transition">Cancel</button>
                    <button onclick="closeDataPreview(true)" class="px-5 py-2 text-sm font-semibold text-white bg-blue-500 hover:bg-blue-600 rounded-lg transition">Looks Good — Continue</button>
                </div>
            </div>`;
        document.body.appendChild(overlay);

    } catch (err) {
        showToast('Failed to load preview: ' + err.message, 'error');
        // Fall through to discovery on error
        await startDiscovery();
    }
}

function closeDataPreview(proceed) {
    const overlay = document.getElementById('data-preview-overlay');
    if (overlay) overlay.remove();
    if (proceed) {
        const pending = state._pendingCache;
        state._pendingCache = null;
        if (pending) {
            showCacheHitDialog(pending.meta, pending.fingerprint);
        } else {
            startDiscovery();
        }
    }
}

/**
 * Trigger Phase 1 discovery.
 */
async function startDiscovery() {
    resetSession();
    state.phase = 'DISCOVER';
    showView('chat-view');
    updatePhaseIndicator('DISCOVER');
    addPhaseBanner('DISCOVER');

    // Disable chat input during discovery
    setChatInputEnabled(false);

    const progress = showInlineProgress('Analyzing your uploaded files...');

    const discoverySteps = [
        [3000,  'Reading spreadsheets and images...'],
        [8000,  'Identifying data structure...'],
        [14000, 'Mapping fields and relationships...'],
    ];
    const discoveryTimers = discoverySteps.map(([ms, msg]) =>
        setTimeout(() => updateInlineProgress(progress, msg), ms)
    );

    try {
        const result = await api('/api/discover', {
            method: 'POST',
            body: JSON.stringify({ job_id: state.jobId }),
        });
        discoveryTimers.forEach(clearTimeout);

        removeInlineProgress(progress);

        // Show discovery response in chat
        addMessage('assistant', result.response);

        // Store file categories in context panel
        if (result.categories) {
            updateContextPanel(result.categories);
        }

        // Add the discovery response to conversation history
        state.conversationHistory.push({
            role: 'assistant',
            content: result.response,
        });

        // Check if cached artifacts let us skip phases
        if (result.has_approved_recipe) {
            // Full reuse — skip interview + recipe, go straight to execution
            showToast('Using your previous recipe.', 'success');
            addSystemMessage('Recognized format — using your saved recipe. Building catalog...');
            setChatInputEnabled(false);
            await buildDataModel();
            return;
        }

        // Enable chat for user to confirm/correct data mapping
        setChatInputEnabled(true);

        // Show the "Confirm data mapping" action button
        showActionButton('confirm-mapping-btn', 'Confirm Data Mapping', async () => {
            hideActionButton('confirm-mapping-btn');
            await buildDataModel();
        });

    } catch (error) {
        removeInlineProgress(progress);
        setChatInputEnabled(true);
        discoveryTimers.forEach(clearTimeout);
        showToast('Discovery failed: ' + error.message, 'error');
        addMessage('assistant', 'Sorry, I encountered an error analyzing your data. Please try sending a message with more details about your files.');
    }
}

// ============================================================================
// Chat (Phases 1-3)
// ============================================================================

/**
 * Add a chat message bubble to the conversation area.
 */
function addMessage(role, content, { html = false } = {}) {
    const chatMessages = document.getElementById('chat-messages');
    if (!chatMessages) return;

    const wrapper = document.createElement('div');
    wrapper.className = `chat-message chat-message-${role}`;

    const bubble = document.createElement('div');
    bubble.className = 'chat-bubble';
    // Trust boundary: html=true must only be used with API-generated content, never raw user input
    bubble.innerHTML = html ? content : formatMessage(content);

    wrapper.appendChild(bubble);
    chatMessages.appendChild(wrapper);

    // Auto-scroll to bottom
    chatMessages.scrollTop = chatMessages.scrollHeight;
}

/**
 * Add a system/phase transition message to chat.
 */
function addSystemMessage(content) {
    const chatMessages = document.getElementById('chat-messages');
    if (!chatMessages) return;

    const wrapper = document.createElement('div');
    wrapper.className = 'flex justify-center my-4';

    const badge = document.createElement('div');
    badge.className = 'bg-slate-100 text-slate-500 text-sm px-4 py-2 rounded-full';
    badge.textContent = content;

    wrapper.appendChild(badge);
    chatMessages.appendChild(wrapper);

    chatMessages.scrollTop = chatMessages.scrollHeight;
}

/**
 * Add a phase banner card to the chat explaining what this phase does.
 */
function addPhaseBanner(phase) {
    const chatMessages = document.getElementById('chat-messages');
    if (!chatMessages) return;

    const banners = {
        DISCOVER: {
            step: '1',
            title: 'Understand Your Products',
            description: 'The AI is reading your files to learn what products you have and match images to listings.',
        },
        INTERVIEW: {
            step: '2',
            title: 'Define Your Brand',
            description: 'Tell me about your selling style — tone, pricing, key details — so listings sound like you.',
        },
        RECIPE_TEST: {
            step: '3',
            title: 'Build Your Listing Recipe',
            description: 'I\'ll create an AI-powered recipe using Gemini to generate listings, test it on a few products, and refine until it\'s right.',
        },
    };

    const b = banners[phase];
    if (!b) return;

    const wrapper = document.createElement('div');
    wrapper.className = 'phase-banner-wrapper';
    wrapper.innerHTML = `
        <div class="phase-banner-divider"></div>
        <div class="phase-banner">
            <div class="flex items-center gap-3">
                <span class="phase-banner-step">${b.step}</span>
                <div>
                    <div class="text-sm font-semibold text-slate-800">${b.title}</div>
                    <div class="text-xs text-slate-500">${b.description}</div>
                </div>
            </div>
        </div>
    `;

    chatMessages.appendChild(wrapper);
    chatMessages.scrollTop = chatMessages.scrollHeight;
}

/**
 * Enable or disable the chat input area.
 */
function setChatInputEnabled(enabled) {
    const input = document.getElementById('chat-input');
    const sendBtn = document.getElementById('send-btn');

    if (input) {
        input.disabled = !enabled;
        if (enabled) {
            input.focus();
        }
    }
    if (sendBtn) {
        sendBtn.disabled = !enabled;
    }
}

/**
 * Show an action button below the chat.
 */
function showActionButton(id, label, onClick) {
    const container = document.getElementById('action-buttons');
    if (!container) return;

    // Remove existing button with same ID if present
    const existing = document.getElementById(id);
    if (existing) existing.remove();

    const btn = document.createElement('button');
    btn.id = id;
    btn.className = 'action-btn btn-primary px-5 py-2.5 rounded-xl text-sm font-semibold flex items-center gap-2';
    btn.innerHTML = `${escapeHtml(label)} <svg class="w-4 h-4" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" d="M13.5 4.5L21 12m0 0l-7.5 7.5M21 12H3"/></svg>`;
    btn.addEventListener('click', onClick);

    container.appendChild(btn);
}

/**
 * Hide (remove) an action button.
 */
function hideActionButton(id) {
    const btn = document.getElementById(id);
    if (btn) btn.remove();
}

/**
 * Send a chat message. Handles different phases.
 */
async function sendMessage() {
    const input = document.getElementById('chat-input');
    if (!input) return;

    const message = input.value.trim();
    if (!message) return;

    // Show user message
    addMessage('user', message);
    input.value = '';
    setChatInputEnabled(false);

    // Add to conversation history
    state.conversationHistory.push({
        role: 'user',
        content: message,
    });

    // Show typing indicator
    const typingIndicator = showTypingIndicator();

    try {
        if (state.phase === 'DISCOVER') {
            // During discovery, user might be providing feedback before
            // confirming the data mapping. Send as chat.
            await handleDiscoveryChat(message);
        } else if (state.phase === 'INTERVIEW') {
            await handleInterviewChat(message);
        } else if (state.phase === 'RECIPE_TEST') {
            await handleRecipeChat(message);
        }
    } catch (error) {
        addMessage('assistant', 'Sorry, something went wrong. Please try again.');
        showToast('Error: ' + error.message, 'error');
    } finally {
        removeTypingIndicator(typingIndicator);
        setChatInputEnabled(true);
    }
}

/**
 * Handle chat during the discovery phase (user confirming/correcting data mapping).
 */
async function handleDiscoveryChat(message) {
    const result = await api('/api/chat', {
        method: 'POST',
        body: JSON.stringify({
            job_id: state.jobId,
            message: message,
            conversation_history: state.conversationHistory,
        }),
    });

    addMessage('assistant', result.response);
    state.conversationHistory.push({
        role: 'assistant',
        content: result.response,
    });

    // Keep the confirm button visible
    showActionButton('confirm-mapping-btn', 'Confirm Data Mapping', async () => {
        hideActionButton('confirm-mapping-btn');
        await buildDataModel();
    });
}

/**
 * Handle chat during the interview phase.
 */
async function handleInterviewChat(message) {
    const result = await api('/api/chat', {
        method: 'POST',
        body: JSON.stringify({
            job_id: state.jobId,
            message: message,
            conversation_history: state.conversationHistory,
        }),
    });

    addMessage('assistant', result.response);
    state.conversationHistory.push({
        role: 'assistant',
        content: result.response,
    });

    // Check if the profile is ready
    if (result.phase === 'profile_ready' || result.phase === 'start_recipe') {
        if (result.style_profile) {
            state.styleProfile = result.style_profile;
            updateContextPanel();
        }
        showActionButton('confirm-profile-btn', 'Confirm Brand Profile', async () => {
            hideActionButton('confirm-profile-btn');
            addSystemMessage('Brand profile confirmed. Building your listing recipe...');
            await startRecipeBuilding();
        });
    } else if (result.style_profile) {
        // Partial profile update
        state.styleProfile = result.style_profile;
        updateContextPanel();
    }

    // Update data model if returned
    if (result.data_model) {
        state.dataModel = result.data_model;
        updateContextPanel();
    }
}

/**
 * Handle chat during recipe testing — user provides feedback to refine the recipe.
 */
async function handleRecipeChat(message) {
    const progress = showInlineProgress('Refining recipe based on your feedback...');

    try {
        const result = await api('/api/chat', {
            method: 'POST',
            body: JSON.stringify({
                job_id: state.jobId,
                message: message,
                conversation_history: state.conversationHistory,
            }),
        });

        removeInlineProgress(progress);

        addMessage('assistant', result.response);
        state.conversationHistory.push({
            role: 'assistant',
            content: result.response,
        });

        // If the backend returned updated test results, refresh them
        if (result.test_results) {
            state.testResults = result.test_results;
            updateContextPanel();
        }

        // If a new recipe was returned
        if (result.recipe) {
            state.recipe = result.recipe;
        }

        // Keep the approve button available
        showActionButton('approve-recipe-btn', 'Approve Recipe', async () => {
            await approveRecipe();
        });

        // Offer to re-test
        showActionButton('test-recipe-btn', 'Re-test Recipe', async () => {
            await testRecipe();
        });

    } catch (error) {
        removeInlineProgress(progress);
        throw error;
    }
}

/**
 * Show a typing indicator in the chat.
 * Returns the indicator element for later removal.
 */
function showTypingIndicator() {
    const chatMessages = document.getElementById('chat-messages');
    if (!chatMessages) return null;

    const wrapper = document.createElement('div');
    wrapper.className = 'chat-message chat-message-assistant typing-indicator';

    const bubble = document.createElement('div');
    bubble.className = 'chat-bubble flex items-center gap-1 py-3';
    bubble.innerHTML = `
        <div class="w-2 h-2 bg-slate-400 rounded-full animate-bounce" style="animation-delay: 0ms"></div>
        <div class="w-2 h-2 bg-slate-400 rounded-full animate-bounce" style="animation-delay: 150ms"></div>
        <div class="w-2 h-2 bg-slate-400 rounded-full animate-bounce" style="animation-delay: 300ms"></div>
    `;

    wrapper.appendChild(bubble);
    chatMessages.appendChild(wrapper);
    chatMessages.scrollTop = chatMessages.scrollHeight;

    return wrapper;
}

/**
 * Remove a typing indicator element.
 */
function removeTypingIndicator(indicator) {
    if (indicator && indicator.parentNode) {
        indicator.parentNode.removeChild(indicator);
    }
}

// ============================================================================
// Build Data Model (transition from discovery to interview)
// ============================================================================

/**
 * Finalize the data model and transition to the interview phase.
 */
async function buildDataModel() {
    setChatInputEnabled(false);
    const progress = showInlineProgress('Preparing to build your catalog...');

    try {
        const response = await fetch('/api/build-data-model', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                job_id: state.jobId,
                conversation_history: state.conversationHistory,
            }),
        });

        if (!response.ok) {
            const err = await response.json().catch(() => ({}));
            throw new Error(err.detail || `Request failed (${response.status})`);
        }

        for await (const event of readSSE(response)) {
            if (event.type === 'progress') {
                updateInlineProgress(progress, event.data.text);
            } else if (event.type === 'complete') {
                const result = event.data;
                state.dataModel = result.data_model;

                const qr = result.quality_report || {};
                const productCount = qr.total_products || (state.dataModel.products || []).length;
                let summary = `Extracted ${productCount} products`;
                const fields = qr.fields_discovered || [];
                if (fields.length) summary += ` with ${fields.length} fields`;
                if (qr.images_matched) summary += `, ${qr.images_matched} images matched`;
                summary += '.';

                const warnings = qr.warnings || [];
                if (warnings.length) {
                    const warningLines = warnings.map(w => `- ${w}`).join('\n');
                    addMessage('assistant', `**Data Quality Report**\n${summary}\n\n**Heads up:**\n${warningLines}\n\nStarting brand profile...`);
                } else {
                    addMessage('assistant', `**Data Quality Report**\n${summary} Everything looks clean.\n\nStarting brand profile...`);
                }
                updateInlineProgress(progress, `Mapped ${productCount} products — starting brand profile...`);

                updateContextPanel();

                // Check if cached artifacts let us skip phases
                const jobStatus = await api(`/api/status/${state.jobId}`);
                if (jobStatus.recipe_approved) {
                    // Full reuse: skip interview + recipe, go to execution
                    removeInlineProgress(progress);
                    addSystemMessage('Using your saved recipe — starting batch execution...');
                    await startExecution();
                } else if (jobStatus.phase === 'building_recipe') {
                    // adjust_style with no style_profile: go to interview normally
                    await startInterview(progress);
                } else {
                    await startInterview(progress);
                }
            } else if (event.type === 'error') {
                removeInlineProgress(progress);
                setChatInputEnabled(true);
                showToast('Failed to build data model: ' + event.data.text, 'error');
                addMessage('assistant', 'I had trouble finalizing the data model. Could you provide more details about how your files are organized?');
            }
        }
    } catch (error) {
        removeInlineProgress(progress);
        setChatInputEnabled(true);
        showToast('Failed to build data model: ' + error.message, 'error');
        addMessage('assistant', 'I had trouble finalizing the data model. Could you provide more details about how your files are organized?');
    }
}

// ============================================================================
// Interview (Phase 2)
// ============================================================================

/**
 * Start the interview phase. Calls the backend to get the opening message.
 */
async function startInterview(existingProgress) {
    state.phase = 'INTERVIEW';
    updatePhaseIndicator('INTERVIEW');

    // Remove old progress so the new one appears below the phase banner
    if (existingProgress) removeInlineProgress(existingProgress);
    addPhaseBanner('INTERVIEW');

    setChatInputEnabled(false);

    const progress = showInlineProgress('AI is preparing interview questions...');

    try {
        // Send a special initial message to trigger the interview
        const result = await api('/api/chat', {
            method: 'POST',
            body: JSON.stringify({
                job_id: state.jobId,
                message: '__start_interview__',
                conversation_history: state.conversationHistory,
            }),
        });

        removeInlineProgress(progress);

        addMessage('assistant', result.response);
        state.conversationHistory.push({
            role: 'assistant',
            content: result.response,
        });

        setChatInputEnabled(true);

    } catch (error) {
        removeInlineProgress(progress);
        setChatInputEnabled(true);
        showToast('Failed to start interview: ' + error.message, 'error');
    }
}

// ============================================================================
// Recipe Building & Testing (Phase 3)
// ============================================================================

/**
 * Transition to recipe building. Called after the style profile is complete.
 */
async function startRecipeBuilding() {
    state.phase = 'RECIPE_TEST';
    updatePhaseIndicator('RECIPE_TEST');
    addPhaseBanner('RECIPE_TEST');

    addMessage('assistant', "Now I'll create a recipe — a set of AI instructions that Gemini will follow to generate each listing. I'll test it on a few of your products and automatically improve it until the quality is high.");

    setChatInputEnabled(false);
    const progress = showInlineProgress('Drafting listing recipe...');

    try {
        const response = await fetch('/api/auto-refine', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ job_id: state.jobId }),
        });

        if (!response.ok) {
            const err = await response.json().catch(() => ({}));
            throw new Error(err.detail || `Request failed (${response.status})`);
        }

        for await (const event of readSSE(response)) {
            if (event.type === 'progress') {
                updateInlineProgress(progress, event.data.text);
            } else if (event.type === 'score') {
                const { attempt, all_passed, details } = event.data;
                const total = details ? details.length : 0;
                const passed = details ? details.filter(d => d.passed).length : 0;
                const status = all_passed
                    ? 'All listings passed!'
                    : `${passed} of ${total} passed — still refining...`;
                updateInlineProgress(progress,
                    `Round ${attempt}: ${status}`);
            } else if (event.type === 'complete') {
                removeInlineProgress(progress);
                handleAutoRefineComplete(event.data);
            } else if (event.type === 'error') {
                removeInlineProgress(progress);
                showToast('Auto-refine error: ' + event.data.text, 'error');
                addMessage('assistant', 'I ran into an issue building the recipe. Could you describe what kind of listing format you prefer?');
                setChatInputEnabled(true);
            }
        }
    } catch (error) {
        removeInlineProgress(progress);
        setChatInputEnabled(true);
        showToast('Recipe building failed: ' + error.message, 'error');
        addMessage('assistant', 'I had trouble creating the recipe. Could you describe what kind of listing format you prefer?');
    }
}

/**
 * Handle the final result from the auto-refine SSE stream.
 */
function handleAutoRefineComplete(data) {
    state.recipe = data.recipe;
    state.testResults = data.test_results || [];
    updateContextPanel();

    const summary = buildRecipeTestSummary(state.testResults);
    const iters = data.iterations;
    const iterLabel = `${iters} round${iters > 1 ? 's' : ''}`;

    const passedCount = (data.test_results || []).filter(tr => tr.validation?.passed).length;
    const totalCount = (data.test_results || []).length;

    if (data.reached_threshold) {
        const footer = `<p style="margin-top:0.75rem">All <strong>${totalCount} test listings passed</strong> after ${iterLabel} of testing. The listings look great!</p>`;
        addMessage('assistant', (summary.html ? summary.content : '') + footer, { html: true });
    } else {
        const footer = `<p style="margin-top:0.75rem">After ${iterLabel}, <strong>${passedCount} of ${totalCount}</strong> test listings passed. Some issues remain (listed above). You can give me specific feedback to improve, or approve as-is.</p>`;
        addMessage('assistant', (summary.html ? summary.content : '') + footer, { html: true });
    }

    state.conversationHistory.push({
        role: 'assistant',
        content: `Recipe tested (${passedCount}/${totalCount} passed, ${data.iterations} iterations).`,
    });

    setChatInputEnabled(true);

    // Show action buttons
    showActionButton('approve-recipe-btn',
        data.reached_threshold ? 'Approve & Start Processing' : 'Approve As-Is',
        async () => { await approveRecipe(); });

    showActionButton('test-recipe-btn', 'Re-test Recipe', async () => {
        await testRecipe();
    });
}

/**
 * Async generator that parses Server-Sent Events from a fetch Response.
 */
async function* readSSE(response) {
    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    let buffer = '';

    try {
        while (true) {
            const { done, value } = await reader.read();
            if (done) break;

            buffer += decoder.decode(value, { stream: true });

            // Process complete events (separated by double newlines)
            const parts = buffer.split('\n\n');
            buffer = parts.pop(); // Keep incomplete part in buffer

            for (const part of parts) {
                if (!part.trim()) continue;

                let eventType = 'message';
                let eventData = '';

                for (const line of part.split('\n')) {
                    if (line.startsWith('event: ')) {
                        eventType = line.slice(7);
                    } else if (line.startsWith('data: ')) {
                        eventData = line.slice(6);
                    }
                }

                if (eventData) {
                    try {
                        yield { type: eventType, data: JSON.parse(eventData) };
                    } catch (e) {
                        console.warn('Failed to parse SSE data:', eventData);
                    }
                }
            }
        }
    } finally {
        reader.releaseLock();
    }
}

/**
 * Build a chat-friendly summary of test results.
 */
function buildRecipeTestSummary(testResults) {
    if (!testResults || testResults.length === 0) {
        return { html: false, content: 'I drafted a recipe but could not test it on any samples. You can approve it or provide feedback.' };
    }

    let cards = '<p style="margin-bottom:0.75rem">Here are the test results for the recipe:</p>';

    testResults.forEach((tr, i) => {
        const name = escapeHtml(tr.product_name || tr.product_id || `Sample ${i + 1}`);
        const score = tr.validation?.score ?? '?';
        const passed = tr.validation?.passed;
        const title = escapeHtml(tr.listing?.title || '(no title generated)');
        const description = tr.listing?.description || '';

        let card = `<div class="test-result-card">`;
        card += `<div class="test-result-header">`;
        card += `<span class="test-result-name">${name}</span>`;
        card += scoreBadgeHtml(score, passed);
        card += `</div>`;
        card += `<div class="test-result-title">${title}</div>`;

        if (description) {
            card += `<div class="test-result-description">${escapeHtml(description)}</div>`;
        }

        card += tagPillsHtml(tr.listing?.tags, 'chat');

        if (tr.listing?.suggested_price) {
            const conf = tr.listing.confidence;
            const confLabel = conf === 'high' ? 'Based on pricing strategy' : conf === 'medium' ? 'Estimated by AI' : 'AI guess';
            card += `<div class="test-result-price">$${escapeHtml(String(tr.listing.suggested_price))} <span class="text-xs text-slate-400 font-normal ml-1">${confLabel}</span></div>`;
        }

        card += criteriaBadgesHtml(tr.validation?.judge_criteria, 'chat');
        card += codeIssuesHtml(tr.validation?.code_issues, 'chat');
        card += `</div>`;
        cards += card;
    });

    const passedCount = testResults.filter(tr => tr.validation?.passed).length;
    cards += `<div class="test-result-avg"><strong>${passedCount} of ${testResults.length}</strong> listings passed</div>`;
    cards += `<p style="margin-top:0.5rem;color:var(--color-text-secondary);font-size:0.85rem">You can approve the recipe or tell me what to change.</p>`;

    return { html: true, content: cards };
}

/**
 * Test (or re-test) the current recipe.
 */
async function testRecipe() {
    hideActionButton('test-recipe-btn');
    hideActionButton('approve-recipe-btn');
    const progress = showInlineProgress('Testing recipe on sample products...');

    try {
        const result = await api('/api/test-recipe', {
            method: 'POST',
            body: JSON.stringify({ job_id: state.jobId }),
        });

        removeInlineProgress(progress);

        state.recipe = result.recipe;
        state.testResults = result.test_results || [];

        const summary = buildRecipeTestSummary(state.testResults);
        addMessage('assistant', summary.content, { html: summary.html });
        state.conversationHistory.push({
            role: 'assistant',
            content: 'Re-tested recipe on sample products.',
        });

        updateContextPanel();

        // Re-show action buttons
        showActionButton('approve-recipe-btn', 'Approve Recipe', async () => {
            await approveRecipe();
        });

        showActionButton('test-recipe-btn', 'Re-test Recipe', async () => {
            await testRecipe();
        });

    } catch (error) {
        removeInlineProgress(progress);
        showToast('Testing failed: ' + error.message, 'error');
    }
}

/**
 * Approve the recipe and transition to execution.
 */
async function approveRecipe() {
    hideActionButton('approve-recipe-btn');
    hideActionButton('test-recipe-btn');
    const progress = showInlineProgress('Locking recipe...');

    try {
        const result = await api('/api/approve-recipe', {
            method: 'POST',
            body: JSON.stringify({ job_id: state.jobId }),
        });

        state.recipe = result.recipe;
        removeInlineProgress(progress);

        addSystemMessage('Recipe approved! Starting batch execution...');

        // Transition to execution
        await startExecution();

    } catch (error) {
        removeInlineProgress(progress);
        showToast('Failed to approve recipe: ' + error.message, 'error');
    }
}

// ============================================================================
// Execution (Phase 4)
// ============================================================================

/**
 * Start batch execution and connect WebSocket.
 */
async function startExecution() {
    state.phase = 'EXECUTING';
    showView('execution-view');
    updatePhaseIndicator('EXECUTING');

    // Reset execution UI
    resetExecutionView();

    try {
        // Connect WebSocket first
        connectWebSocket();

        // Start execution
        await api('/api/execute', {
            method: 'POST',
            body: JSON.stringify({ job_id: state.jobId }),
        });

    } catch (error) {
        showToast('Execution failed to start: ' + error.message, 'error');
    }
}

/**
 * Reset the execution view to initial state.
 */
function resetExecutionView() {
    // Progress bar
    const progressBar = document.getElementById('progress-bar');
    const progressText = document.getElementById('progress-text');
    if (progressBar) progressBar.style.width = '0%';
    if (progressText) progressText.textContent = '0 / 0 products';

    // Clear listings grid
    const grid = document.getElementById('listings-grid');
    if (grid) grid.innerHTML = '';

    // Reset stats
    const statsContainer = document.getElementById('execution-stats');
    if (statsContainer) statsContainer.innerHTML = '';

    // Hide download buttons
    const downloadSection = document.getElementById('download-section');
    if (downloadSection) downloadSection.classList.add('hidden');
}

/**
 * Connect to the WebSocket for progress updates.
 */
function connectWebSocket() {
    if (state.ws) {
        state.ws.close();
        state.ws = null;
    }

    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const wsUrl = `${protocol}//${window.location.host}/ws/${state.jobId}`;

    try {
        state.ws = new WebSocket(wsUrl);

        state.ws.onopen = () => {
            console.log('WebSocket connected');
        };

        state.ws.onmessage = (event) => {
            try {
                const data = JSON.parse(event.data);
                handleWSMessage(data);
            } catch (e) {
                console.error('Failed to parse WebSocket message:', e);
            }
        };

        state.ws.onclose = () => {
            console.log('WebSocket closed');
            // If we are still executing, try to reconnect after a delay
            if (state.phase === 'EXECUTING') {
                setTimeout(() => {
                    if (state.phase === 'EXECUTING') {
                        connectWebSocket();
                    }
                }, 3000);
            }
        };

        state.ws.onerror = (error) => {
            console.error('WebSocket error:', error);
        };

    } catch (error) {
        console.error('Failed to connect WebSocket:', error);
        // Fall back to polling
        startPolling();
    }
}

/**
 * Handle incoming WebSocket messages.
 */
function handleWSMessage(data) {
    switch (data.type) {
        case 'batch_start':
            handleBatchStart(data);
            break;
        case 'progress':
            handleProgress(data);
            break;
        case 'batch_complete':
            handleBatchComplete(data);
            break;
        default:
            console.log('Unknown WS message type:', data.type);
    }
}

/**
 * Handle the batch_start WebSocket event.
 */
function handleBatchStart(data) {
    const progressText = document.getElementById('progress-text');
    if (progressText) {
        progressText.textContent = `0 / ${data.total} products`;
    }
}

/**
 * Handle a progress update from the WebSocket.
 */
function handleProgress(data) {
    const { product_id, completed, total, score, title, status } = data;

    // Update progress bar (skip for "retrying" — no completion yet)
    if (status !== 'retrying' && completed != null) {
        const progressBar = document.getElementById('progress-bar');
        const progressText = document.getElementById('progress-text');

        if (progressBar && total > 0) {
            const pct = Math.round((completed / total) * 100);
            progressBar.style.width = `${pct}%`;
        }

        if (progressText) {
            progressText.textContent = `${completed} / ${total} products`;
        }
    }

    // Add or update listing card
    addListingCard({
        product_id,
        title: title || product_id,
        score,
        status,
        issues: data.issues || [],
    });

    // Store result (only final results, not intermediate retrying)
    if (status !== 'retrying') {
        state.batchResults.push(data);
    }

    // Update live stats
    const succeeded = state.batchResults.filter(r => !r.failed && r.status !== 'failed').length;
    const failed = state.batchResults.filter(r => r.failed || r.status === 'failed').length;
    const retried = state.batchResults.filter(r => r.retried || r.status === 'retried').length;
    showExecutionStats({ succeeded, failed, retried });
}

/**
 * Handle the batch_complete WebSocket event.
 */
function handleBatchComplete(data) {
    state.phase = 'RESULTS';
    state.executionStats = data.report;

    // Close WebSocket
    if (state.ws) {
        state.ws.close();
        state.ws = null;
    }

    // Update progress to 100%
    const progressBar = document.getElementById('progress-bar');
    if (progressBar) progressBar.style.width = '100%';

    // Update execution badge
    const badge = document.getElementById('execution-badge');
    if (badge) {
        badge.textContent = 'Complete';
        badge.className = 'badge badge-success';
    }

    // Show stats
    showExecutionStats(data.report);

    // Show download ZIP button
    const downloadZipBtn = document.getElementById('download-zip-btn');
    if (downloadZipBtn) downloadZipBtn.classList.remove('hidden');
    if (downloadZipBtn) downloadZipBtn.classList.add('flex');

    // Show export section
    const exportSection = document.getElementById('export-section');
    if (exportSection) exportSection.classList.remove('hidden');

    // Load full listings data for the detail modal
    loadListingsData();

    showToast('Batch execution complete!', 'success');
}

/**
 * Load full listings data from the API for the detail modal.
 */
async function loadListingsData() {
    if (!state.jobId) return;
    try {
        const data = await api(`/api/listings/${state.jobId}`);
        state.fullListings = data.listings || [];
    } catch (e) {
        console.warn('Failed to load full listings data:', e);
        state.fullListings = [];
    }
}

/**
 * Display execution statistics (live during progress & final report).
 */
function showExecutionStats(report) {
    const container = document.getElementById('execution-stats');
    if (!container || !report) return;

    const elapsed = report.elapsed_seconds
        ? `<div class="mt-3 text-sm text-slate-500 text-center">Completed in ${report.elapsed_seconds}s</div>`
        : '';

    container.innerHTML = `
        <div class="grid grid-cols-3 gap-4">
            <div class="bg-slate-50 rounded-xl p-4 border border-slate-100">
                <p class="text-xs font-medium text-slate-400 uppercase tracking-wider mb-1">Succeeded</p>
                <p class="text-2xl font-bold text-green-600">${report.succeeded}</p>
            </div>
            <div class="bg-slate-50 rounded-xl p-4 border border-slate-100">
                <p class="text-xs font-medium text-slate-400 uppercase tracking-wider mb-1">Failed</p>
                <p class="text-2xl font-bold text-red-500">${report.failed}</p>
            </div>
            <div class="bg-slate-50 rounded-xl p-4 border border-slate-100">
                <p class="text-xs font-medium text-slate-400 uppercase tracking-wider mb-1">Retried</p>
                <p class="text-2xl font-bold text-amber-500">${report.retried}</p>
            </div>
        </div>
        ${elapsed}
    `;
}

/**
 * Add a listing card to the execution results grid.
 */
function addListingCard(data) {
    const grid = document.getElementById('listings-grid');
    if (!grid) return;

    const isRetrying = data.status === 'retrying';
    const isOk = data.status === 'ok';

    // Check if a card already exists for this product (update in-place)
    let card = grid.querySelector(`[data-product-id="${data.product_id}"]`);
    const isNew = !card;

    if (isNew) {
        card = document.createElement('div');
        card.dataset.productId = data.product_id;

        // Make card clickable to open detail modal
        card.addEventListener('click', () => {
            if (card.dataset.retrying === 'true') return; // don't open while retrying
            const result = (state.fullListings || []).find(
                l => l.product_id === data.product_id
            );
            if (result) {
                openListingDetail(result);
            } else {
                showToast('Loading listing details...', 'info');
                loadListingsData().then(() => {
                    const r = (state.fullListings || []).find(
                        l => l.product_id === data.product_id
                    );
                    if (r) openListingDetail(r);
                    else showToast('Could not load listing details.', 'error');
                });
            }
        });
    }

    card.dataset.retrying = isRetrying ? 'true' : 'false';
    card.className = isRetrying
        ? 'listing-card view-enter'
        : 'listing-card view-enter clickable';

    if (isRetrying) {
        // Show spinner + issues while retrying
        const issuesHtml = (data.issues || []).slice(0, 3).map(
            i => `<li class="text-[11px] text-amber-600 leading-snug">${escapeHtml(i)}</li>`
        ).join('');

        card.innerHTML = `
            <div class="flex items-start justify-between mb-2">
                <div class="flex items-center gap-2">
                    <div class="spinner spinner-sm"></div>
                    <span class="text-xs text-slate-400 font-mono">${data.product_id}</span>
                </div>
                <span class="text-xs font-semibold px-2 py-0.5 rounded-full text-amber-600 bg-amber-50">
                    Retrying...
                </span>
            </div>
            <h3 class="listing-card-title text-sm">${escapeHtml(data.title || 'Generating...')}</h3>
            ${issuesHtml ? `<ul class="mt-2 space-y-0.5 list-disc list-inside">${issuesHtml}</ul>` : ''}
            <p class="text-xs text-slate-400 mt-2">Fixing issues and regenerating</p>
        `;
    } else {
        // Final state: ok or failed
        const cardStatusColor = isOk ? 'text-green-600 bg-green-50' : 'text-amber-600 bg-amber-50';
        const cardStatusLabel = isOk ? 'Checked — OK' : 'Needs work';

        const statusIcon = isOk
            ? '<svg class="w-4 h-4 text-green-500" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 13l4 4L19 7"/></svg>'
            : '<svg class="w-4 h-4 text-red-500" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"/></svg>';

        card.innerHTML = `
            <div class="flex items-start justify-between mb-2">
                <div class="flex items-center gap-2">
                    ${statusIcon}
                    <span class="text-xs text-slate-400 font-mono">${data.product_id}</span>
                </div>
                <span class="text-xs font-semibold px-2 py-0.5 rounded-full ${cardStatusColor}">
                    ${cardStatusLabel}
                </span>
            </div>
            <h3 class="listing-card-title text-sm">${escapeHtml(data.title || 'Untitled')}</h3>
            <p class="text-xs text-slate-400 mt-2">Click to view full listing</p>
        `;
    }

    if (isNew) {
        grid.appendChild(card);
    }

    // Scroll grid to show newest
    grid.scrollTop = grid.scrollHeight;
}

/**
 * Escape HTML special characters.
 */
function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

/**
 * Generate a copy-to-clipboard button. `copyExpr` is a JS expression that
 * evaluates to the string to copy (e.g. a quoted literal or a DOM lookup).
 */
function copyButtonHtml(copyExpr) {
    return `<button class="copy-btn" onclick="copyToClipboard(${copyExpr}, this)">
        <svg class="w-3 h-3" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" d="M15.666 3.888A2.25 2.25 0 0013.5 2.25h-3c-1.03 0-1.9.693-2.166 1.638m7.332 0c.055.194.084.4.084.612v0a.75.75 0 01-.75.75H9.75a.75.75 0 01-.75-.75v0c0-.212.03-.418.084-.612m7.332 0c.646.049 1.288.11 1.927.184 1.1.128 1.907 1.077 1.907 2.185V19.5a2.25 2.25 0 01-2.25 2.25H6.75A2.25 2.25 0 014.5 19.5V6.257c0-1.108.806-2.057 1.907-2.185a48.208 48.208 0 011.927-.184"/></svg>
        Copy
    </button>`;
}

/**
 * Render a score badge with appropriate color.
 */
function scoreBadgeHtml(score, passed) {
    const color = passed ? '#059669' : '#d97706';
    const label = passed ? 'Checked — OK' : 'Needs work';
    return `<span class="test-result-score" style="background:${color}">${label}</span>`;
}

/**
 * Render judge criteria as inline badges.
 */
function criteriaBadgesHtml(judgeCriteria, style = 'chat') {
    if (!judgeCriteria || judgeCriteria.length === 0) return '';

    if (style === 'panel') {
        const passed = judgeCriteria.filter(c => c.pass).length;
        return `
            <div class="mt-2 pt-2 border-t border-slate-100">
                <div class="text-[10px] text-slate-400 mb-1 uppercase tracking-wide">Quality ${passed}/${judgeCriteria.length}</div>
                <div class="flex flex-wrap gap-1">
                    ${judgeCriteria.map(c => {
                        const color = c.pass ? 'bg-green-50 text-green-600' : 'bg-red-50 text-red-600';
                        const icon = c.pass ? '&#10003;' : '&#10007;';
                        const label = c.criterion.replace(/_/g, ' ');
                        return `<span class="text-[10px] px-1.5 py-0.5 rounded ${color}" title="${escapeHtml(c.reasoning || '')}">${icon} ${label}</span>`;
                    }).join('')}
                </div>
            </div>
        `;
    }

    // chat style
    return `<div class="test-result-criteria">
        ${judgeCriteria.map(c => {
            const cls = c.pass ? 'criterion-pass' : 'criterion-fail';
            const icon = c.pass ? '&#10003;' : '&#10007;';
            const label = (c.label || c.criterion || '').replace(/_/g, ' ');
            const tip = c.pass ? '' : ` title="${escapeHtml(c.reasoning?.slice(0, 150))}"`;
            return `<span class="test-result-criterion ${cls}"${tip}>${icon} ${escapeHtml(label)}</span>`;
        }).join('')}
    </div>`;
}

/**
 * Render code/structural issues as warning badges.
 */
function codeIssuesHtml(issues, style = 'chat') {
    if (!issues || issues.length === 0) return '';

    if (style === 'panel') {
        return `<div class="mt-1.5 space-y-0.5">
            ${issues.map(i => `<div class="text-[10px] text-red-500">- ${escapeHtml(i)}</div>`).join('')}
        </div>`;
    }

    return `<div class="test-result-issues">
        ${issues.map(issue => `<span class="test-result-issue">&#9888; ${escapeHtml(issue)}</span>`).join('')}
    </div>`;
}

/**
 * Render tags as small pills.
 */
function tagPillsHtml(tags, style = 'chat') {
    if (!tags || tags.length === 0) return '';

    if (style === 'panel') {
        return `<div class="flex flex-wrap gap-1 mb-2">${tags.slice(0, 8).map(t =>
            `<span class="text-[10px] px-1.5 py-0.5 rounded bg-slate-100 text-slate-500">${escapeHtml(t)}</span>`
        ).join('')}${tags.length > 8 ? `<span class="text-[10px] text-slate-400">+${tags.length - 8}</span>` : ''}</div>`;
    }

    return `<div class="test-result-tags">
        ${tags.map(tag => `<span class="test-result-tag">${escapeHtml(tag)}</span>`).join('')}
    </div>`;
}

/**
 * Fallback polling for when WebSocket is unavailable.
 */
function startPolling() {
    const pollInterval = setInterval(async () => {
        if (state.phase !== 'EXECUTING') {
            clearInterval(pollInterval);
            return;
        }

        try {
            const status = await api(`/api/status/${state.jobId}`);
            if (status.phase === 'complete') {
                clearInterval(pollInterval);
                handleBatchComplete({ report: status.report || {} });
            }
        } catch (e) {
            // Silently continue polling
        }
    }, 3000);
}

// ============================================================================
// Context Panel
// ============================================================================

/**
 * Update the context panel based on the current phase and available data.
 */
function updateContextPanel(categories) {
    const panel = document.getElementById('context-content');
    if (!panel) return;

    let html = '';

    // File summary section (always show if we have upload info)
    if (categories || state.dataModel) {
        html += renderFileContextSection(categories);
    }

    // Data model section
    if (state.dataModel) {
        html += renderDataModelSection();
    }

    // Style profile section
    if (state.styleProfile) {
        html += renderStyleProfileSection();
    }

    // Test results section
    if (state.testResults && state.testResults.length > 0) {
        html += renderTestResultsSection();
    }

    if (html) {
        panel.innerHTML = html;
    } else {
        panel.innerHTML = `
            <div class="text-center text-slate-400 py-8">
                <svg class="w-12 h-12 mx-auto mb-3 text-slate-300" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="1.5" d="M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z"/>
                </svg>
                <p class="text-sm">Context will appear here as we work through your data.</p>
            </div>
        `;
    }
}

/**
 * Render file summary in context panel.
 */
function renderFileContextSection(categories) {
    if (!categories) return '';

    const counts = [];
    if (categories.images?.length) counts.push(`${categories.images.length} images`);
    if (categories.spreadsheets?.length) counts.push(`${categories.spreadsheets.length} spreadsheets`);
    if (categories.documents?.length) counts.push(`${categories.documents.length} documents`);
    if (categories.other?.length) counts.push(`${categories.other.length} other files`);

    return `
        <div class="context-section">
            <div class="context-section-title">Uploaded Files</div>
            <div class="context-item">
                <span class="text-sm">${categories.summary || counts.join(', ')}</span>
            </div>
        </div>
    `;
}

/**
 * Render a single field completeness bar for the context panel.
 */
function renderFieldBar(field, completeness, stats) {
    const pct = completeness.pct ?? 100;
    let detail = '';
    if (stats.type === 'numeric') {
        detail = `${stats.min}–${stats.max}`;
    } else if (stats.unique_count) {
        detail = `${stats.unique_count} unique`;
    }
    const barColor = pct === 100 ? 'bg-green-400' : pct >= 80 ? 'bg-blue-400' : 'bg-amber-400';
    return `
        <div class="flex items-center gap-2 py-0.5">
            <span class="text-xs text-slate-500 w-24 truncate" title="${escapeHtml(field)}">${escapeHtml(field)}</span>
            <div class="flex-1 h-1.5 bg-slate-100 rounded-full overflow-hidden">
                <div class="${barColor} h-full rounded-full" style="width:${pct}%"></div>
            </div>
            <span class="text-xs text-slate-400 w-16 text-right">${detail || pct + '%'}</span>
        </div>
    `;
}

/**
 * Format a style profile as a visual card for the chat.
 */
function formatStyleProfileCard(profile) {
    if (!profile) return '<p>No profile data available.</p>';

    const pill = (text) => `<span style="display:inline-block;background:#EEF2FF;color:#4F46E5;font-size:12px;padding:3px 10px;border-radius:999px;margin:2px 4px 2px 0">${escapeHtml(String(text))}</span>`;

    const formatLabel = (key) => key.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase());

    // Build a title from platform + seller_type if available
    const titleParts = [profile.platform, profile.seller_type].filter(Boolean);
    const title = titleParts.length ? titleParts.map(s => escapeHtml(s)).join(' · ') : 'Brand Profile';

    // Render all fields dynamically — label stacked above value for consistency
    let rowsHtml = '';
    for (const [key, value] of Object.entries(profile)) {
        if (value === null || value === undefined || value === '') continue;
        if (key === 'platform' || key === 'seller_type') continue; // already in title

        if (Array.isArray(value)) {
            const items = value.filter(v => v !== null && v !== '');
            if (!items.length) continue;
            rowsHtml += `<div style="padding:6px 0">
                <div style="color:#64748B;font-size:11px;text-transform:uppercase;letter-spacing:0.05em;margin-bottom:3px">${escapeHtml(formatLabel(key))}</div>
                <div>${items.map(v => pill(v)).join('')}</div>
            </div>`;
        } else {
            rowsHtml += `<div style="padding:6px 0">
                <div style="color:#64748B;font-size:11px;text-transform:uppercase;letter-spacing:0.05em;margin-bottom:1px">${escapeHtml(formatLabel(key))}</div>
                <div style="color:#1E293B;font-size:13px">${escapeHtml(String(value))}</div>
            </div>`;
        }
    }

    return `
        <p style="margin-bottom:10px">Here's your brand profile. Review it and click <strong>Confirm Brand Profile</strong> to proceed, or tell me what to adjust.</p>
        <div style="font-weight:600;font-size:14px;color:#1E293B;margin-bottom:6px;padding-bottom:6px;border-bottom:1px solid #E2E8F0">${title}</div>
        <div style="display:grid;grid-template-columns:1fr 1fr;gap:0 20px">${rowsHtml}</div>
    `;
}

/**
 * Render a single product row for the context panel.
 */
function renderProductRow(product, nameField) {
    const name = product[nameField] || product.name || product.sku || product.id;
    const category = nameField !== 'Category' && product.Category ? product.Category : '';
    const hasImage = product.image_files && product.image_files.length > 0;
    const imageIcon = hasImage
        ? '<svg class="w-3 h-3 text-green-500 shrink-0" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 13l4 4L19 7"/></svg>'
        : '<svg class="w-3 h-3 text-slate-300 shrink-0" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 16l4.586-4.586a2 2 0 012.828 0L16 16m-2-2l1.586-1.586a2 2 0 012.828 0L20 14m-6-6h.01M6 20h12a2 2 0 002-2V6a2 2 0 00-2-2H6a2 2 0 00-2 2v12a2 2 0 002 2z"/></svg>';
    const categoryBadge = category
        ? `<span class="text-[10px] text-slate-400 shrink-0">${escapeHtml(category)}</span>`
        : '';

    return `
        <div class="context-item flex items-center gap-2 min-w-0">
            ${imageIcon}
            <span class="text-sm truncate flex-1">${escapeHtml(String(name))}</span>
            ${categoryBadge}
        </div>
    `;
}

/**
 * Render data model info in context panel.
 */
function renderDataModelSection() {
    if (!state.dataModel) return '';

    const products = state.dataModel.products || [];
    const unmatched = state.dataModel.unmatched_images || [];
    const qr = state.dataModel.quality_report || {};
    const fieldStats = state.dataModel.field_stats || {};
    const fieldsDiscovered = state.dataModel.fields_discovered || [];

    // Field completeness bars
    let fieldsHtml = '';
    if (fieldsDiscovered.length > 0) {
        const fieldCompleteness = qr.field_completeness || {};
        fieldsHtml = fieldsDiscovered.slice(0, 10)
            .map(f => renderFieldBar(f, fieldCompleteness[f] || {}, fieldStats[f] || {}))
            .join('');
        if (fieldsDiscovered.length > 10) {
            fieldsHtml += `<div class="text-xs text-slate-400 text-center py-1">+ ${fieldsDiscovered.length - 10} more fields</div>`;
        }
    }

    // Product list — prefer descriptive fields over generic ones
    const namePreference = ['item', 'name', 'title', 'product_name', 'product', 'description', 'sku'];
    const nameField = namePreference.find(n => fieldsDiscovered.includes(n))
        || fieldsDiscovered.find(f => {
            const s = fieldStats[f];
            return s && s.type === 'text' && s.unique_count === products.length;
        })
        || fieldsDiscovered.find(f => fieldStats[f]?.type === 'text');
    let productsHtml = products.slice(0, 8)
        .map(p => renderProductRow(p, nameField))
        .join('');
    if (products.length > 8) {
        productsHtml += `<div class="text-xs text-slate-400 text-center py-2">+ ${products.length - 8} more products</div>`;
    }

    // Warnings
    const warnings = qr.warnings || [];
    const warningsHtml = warnings.map(w => `
        <div class="context-item bg-amber-50 border-amber-200">
            <span class="text-xs text-amber-700">${escapeHtml(w)}</span>
        </div>
    `).join('');

    let unmatchedHtml = '';
    if (unmatched.length > 0 && !warnings.length) {
        unmatchedHtml = `
            <div class="context-item bg-amber-50 border-amber-200">
                <span class="text-sm text-amber-700">${unmatched.length} unmatched image${unmatched.length !== 1 ? 's' : ''}</span>
            </div>
        `;
    }

    const imgCount = products.filter(p => p.image_files && p.image_files.length > 0).length;
    const productDesc = imgCount > 0
        ? `Extracted from your data \u00b7 ${imgCount}/${products.length} with images`
        : 'Extracted from your data';

    return `
        <div class="context-section">
            <div class="context-section-title">Products (${products.length})</div>
            <div class="text-xs text-slate-400 mb-2">${productDesc}</div>
            ${productsHtml}
            ${unmatchedHtml}
        </div>
        ${fieldsHtml ? `
        <div class="context-section">
            <div class="context-section-title">Fields (${fieldsDiscovered.length})</div>
            ${fieldsHtml}
        </div>
        ` : ''}
        ${warningsHtml ? `
        <div class="context-section">
            <div class="context-section-title">Warnings</div>
            ${warningsHtml}
        </div>
        ` : ''}
    `;
}

/**
 * Render style profile in context panel.
 */
function renderStyleProfileSection() {
    if (!state.styleProfile) return '';

    const fields = [
        { label: 'Platform', key: 'platform' },
        { label: 'Seller Type', key: 'seller_type' },
        { label: 'Target Buyer', key: 'target_buyer' },
        { label: 'Voice', key: 'brand_voice' },
        { label: 'Pricing', key: 'pricing_strategy' },
        { label: 'Title Format', key: 'title_format' },
        { label: 'Description', key: 'description_structure' },
    ];

    let fieldsHtml = '';
    fields.forEach(f => {
        const value = state.styleProfile[f.key];
        if (value) {
            fieldsHtml += `
                <div class="context-item">
                    <div class="text-xs text-slate-400 mb-0.5">${f.label}</div>
                    <div class="text-sm">${escapeHtml(String(value))}</div>
                </div>
            `;
        }
    });

    const mentions = state.styleProfile.always_mention || [];
    if (mentions.length > 0) {
        fieldsHtml += `
            <div class="context-item">
                <div class="text-xs text-slate-400 mb-0.5">Always Mention</div>
                <div class="text-sm">${mentions.map(m => escapeHtml(m)).join(', ')}</div>
            </div>
        `;
    }

    return `
        <div class="context-section">
            <div class="context-section-title">Style Profile</div>
            ${fieldsHtml}
        </div>
    `;
}

/**
 * Render test results in context panel.
 */
function renderTestResultsSection() {
    if (!state.testResults || state.testResults.length === 0) return '';

    let cardsHtml = '';
    state.testResults.forEach(tr => {
        const name = tr.product_name || tr.product_id || 'Unknown';
        const score = tr.validation?.score ?? 0;
        const passed = tr.validation?.passed;
        const listing = tr.listing || {};

        const statusColor = passed ? 'bg-green-100 text-green-700' : 'bg-amber-100 text-amber-700';
        const statusLabel = passed ? 'Checked — OK' : 'Needs work';

        const statusBadge = passed
            ? '<span class="badge badge-success text-xs">Passed</span>'
            : '<span class="badge badge-error text-xs">Needs work</span>';

        const titleHtml = listing.title
            ? `<div class="text-xs font-medium text-slate-700 mb-1">"${escapeHtml(listing.title)}"</div>`
            : '';

        const desc = listing.description || '';
        const descPreview = desc.length > 150 ? desc.slice(0, 150) + '...' : desc;
        const descHtml = desc
            ? `<div class="text-xs text-slate-500 mb-2 leading-relaxed">${escapeHtml(descPreview)}</div>`
            : '';

        const priceConf = listing.confidence;
        const priceConfLabel = priceConf === 'high' ? 'based on pricing strategy' : priceConf === 'medium' ? 'estimated by AI' : 'AI guess';
        const priceHtml = listing.suggested_price
            ? `<div class="text-xs text-slate-500 mb-2">AI price: <span class="font-semibold text-slate-700">$${listing.suggested_price}</span> <span class="text-slate-400">(${priceConfLabel})</span></div>`
            : '';

        cardsHtml += `
            <div class="context-item">
                <div class="flex items-center justify-between mb-1">
                    <span class="text-sm font-medium truncate">${escapeHtml(name)}</span>
                    <span class="text-xs font-bold px-2 py-0.5 rounded-full ${statusColor}">${statusLabel}</span>
                </div>
                ${titleHtml}
                ${descHtml}
                ${tagPillsHtml(listing.tags, 'panel')}
                ${priceHtml}
                ${statusBadge}
                ${codeIssuesHtml(tr.validation?.code_issues, 'panel')}
                ${criteriaBadgesHtml(tr.validation?.judge_criteria, 'panel')}
            </div>
        `;
    });

    const panelPassedCount = state.testResults.filter(tr => tr.validation?.passed).length;

    return `
        <div class="context-section">
            <div class="context-section-title">
                Test Results
                <span class="float-right text-xs font-normal normal-case">${panelPassedCount}/${state.testResults.length} passed</span>
            </div>
            ${cardsHtml}
        </div>
    `;
}

// ============================================================================
// Download & Export
// ============================================================================

/**
 * Trigger download of the output ZIP.
 */
function downloadResults() {
    if (!state.jobId) {
        showToast('No job ID available.', 'error');
        return;
    }

    const link = document.createElement('a');
    link.href = `/api/download/${state.jobId}`;
    link.download = `listings-${state.jobId}.zip`;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
}

/**
 * Download a platform-specific export format.
 */
function downloadFormat(format) {
    if (!state.jobId) {
        showToast('No job ID available.', 'error');
        return;
    }

    const link = document.createElement('a');
    link.href = `/api/download/${state.jobId}/${format}`;
    link.download = '';
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);

    const labels = {
        etsy: 'Etsy CSV',
        ebay: 'eBay CSV',
        shopify: 'Shopify CSV',
        csv: 'Full CSV',
        text: 'Copy-Paste Text',
    };
    showToast(`Downloading ${labels[format] || format}...`, 'success');
}

/**
 * Copy text to clipboard and show feedback on the button.
 */
function copyToClipboard(text, btnEl) {
    navigator.clipboard.writeText(text).then(() => {
        if (btnEl) {
            const original = btnEl.innerHTML;
            btnEl.classList.add('copied');
            btnEl.innerHTML = `<svg class="w-3 h-3" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" d="M5 13l4 4L19 7"/></svg> Copied`;
            setTimeout(() => {
                btnEl.classList.remove('copied');
                btnEl.innerHTML = original;
            }, 1500);
        }
    }).catch(() => {
        showToast('Copy failed — try selecting the text manually.', 'error');
    });
}

// ============================================================================
// Listing Detail Modal
// ============================================================================

/**
 * Open a detailed view of a single listing with copy buttons for each field.
 */
function openListingDetail(result) {
    const listing = result.listing || {};
    const esc = s => escapeHtml(String(s || ''));

    // Build specifics grid
    const specifics = listing.item_specifics || {};
    let specificsHtml = '';
    if (Object.keys(specifics).length > 0) {
        const specificsText = Object.entries(specifics).map(([k,v]) => `${k}: ${v}`).join('\\n');
        specificsHtml = `
            <div class="listing-modal-section">
                <div class="listing-modal-section-title">
                    <span>Item Specifics</span>
                    ${copyButtonHtml(`\`${specificsText}\``)}
                </div>
                <div class="specifics-grid">
                    ${Object.entries(specifics).map(([k, v]) => `
                        <div class="specifics-item">
                            <div class="specifics-item-key">${esc(k)}</div>
                            <div class="specifics-item-value">${esc(v)}</div>
                        </div>
                    `).join('')}
                </div>
            </div>
        `;
    }

    // Build hashtags
    const hashtags = listing.hashtags || [];
    let hashtagsHtml = '';
    if (hashtags.length > 0) {
        const hashtagStr = hashtags.map(h => `#${h}`).join(' ');
        hashtagsHtml = `
            <div class="listing-modal-section">
                <div class="listing-modal-section-title">
                    <span>Hashtags (${hashtags.length})</span>
                    ${copyButtonHtml(`'${hashtagStr.replace(/'/g, "\\'")}'`)}
                </div>
                <div class="flex flex-wrap gap-1">
                    ${hashtags.map(h => `<span class="hashtag-pill">#${esc(h)}</span>`).join('')}
                </div>
            </div>
        `;
    }

    // Build social caption
    const caption = listing.social_caption || '';
    let captionHtml = '';
    if (caption) {
        captionHtml = `
            <div class="listing-modal-section">
                <div class="listing-modal-section-title">
                    <span>Social Media Caption</span>
                    ${copyButtonHtml("document.getElementById('modal-caption-text').textContent")}
                </div>
                <div class="listing-modal-section-content" id="modal-caption-text">${esc(caption)}</div>
            </div>
        `;
    }

    // Build condition
    const condition = listing.condition_description || '';
    let conditionHtml = '';
    if (condition) {
        conditionHtml = `
            <div class="listing-modal-section">
                <div class="listing-modal-section-title">
                    <span>Condition</span>
                    ${copyButtonHtml("document.getElementById('modal-condition-text').textContent")}
                </div>
                <div class="listing-modal-section-content" id="modal-condition-text">${esc(condition)}</div>
            </div>
        `;
    }

    // Tags section
    const tags = listing.tags || [];
    const tagsStr = tags.join(', ');

    // Price and confidence
    const price = listing.suggested_price;
    const confidence = listing.confidence;
    const priceRationale = listing.pricing_rationale || '';
    const priceConfLabel = confidence === 'high' ? 'Based on pricing strategy' : confidence === 'medium' ? 'Estimated by AI' : 'AI guess';

    const overlay = document.createElement('div');
    overlay.className = 'listing-modal-overlay';
    overlay.id = 'listing-modal-overlay';
    overlay.addEventListener('click', (e) => {
        if (e.target === overlay) closeListingDetail();
    });

    overlay.innerHTML = `
        <div class="listing-modal">
            <div class="listing-modal-header">
                <div>
                    <h2 class="text-lg font-bold text-slate-900">${esc(result.sku || result.product_id)}</h2>
                    ${price ? `
                    <div class="flex items-center gap-2 mt-1">
                        <span class="text-lg font-bold text-blue-600">$${price}</span>
                        <span class="text-xs text-slate-400">${priceConfLabel}</span>
                    </div>
                    ${priceRationale ? `<div class="text-xs text-slate-400 mt-0.5">${esc(priceRationale)}</div>` : ''}
                    ` : ''}
                </div>
                <button onclick="closeListingDetail()" class="p-2 hover:bg-slate-100 rounded-lg transition">
                    <svg class="w-5 h-5 text-slate-400" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" d="M6 18L18 6M6 6l12 12"/></svg>
                </button>
            </div>
            <div class="listing-modal-body">
                <div class="listing-modal-section">
                    <div class="listing-modal-section-title">
                        <span>Title</span>
                        ${copyButtonHtml("document.getElementById('modal-title-text').textContent")}
                    </div>
                    <div class="listing-modal-section-content font-semibold" id="modal-title-text">${esc(listing.title)}</div>
                </div>

                <div class="listing-modal-section">
                    <div class="listing-modal-section-title">
                        <span>Description</span>
                        ${copyButtonHtml("document.getElementById('modal-desc-text').textContent")}
                    </div>
                    <div class="listing-modal-section-content" id="modal-desc-text">${esc(listing.description)}</div>
                </div>

                ${tags.length > 0 ? `
                <div class="listing-modal-section">
                    <div class="listing-modal-section-title">
                        <span>Tags (${tags.length})</span>
                        ${copyButtonHtml(`'${tagsStr.replace(/'/g, "\\'")}'`)}
                    </div>
                    <div class="flex flex-wrap gap-1.5">
                        ${tags.map(t => `<span class="text-xs px-2 py-1 rounded-lg bg-slate-100 text-slate-600 border border-slate-200">${esc(t)}</span>`).join('')}
                    </div>
                </div>
                ` : ''}

                ${conditionHtml}
                ${specificsHtml}
                ${captionHtml}
                ${hashtagsHtml}

                ${priceRationale ? `
                <div class="listing-modal-section">
                    <div class="listing-modal-section-title"><span>Pricing Rationale</span></div>
                    <div class="listing-modal-section-content text-sm text-slate-500">${esc(priceRationale)}</div>
                </div>
                ` : ''}

                ${listing.notes_for_seller ? `
                <div class="listing-modal-section">
                    <div class="listing-modal-section-title"><span>Notes for Seller</span></div>
                    <div class="listing-modal-section-content text-sm text-amber-700 bg-amber-50 border-amber-200">${esc(listing.notes_for_seller)}</div>
                </div>
                ` : ''}
            </div>
        </div>
    `;

    document.body.appendChild(overlay);

    // Close on Escape key
    const escHandler = (e) => {
        if (e.key === 'Escape') {
            closeListingDetail();
            document.removeEventListener('keydown', escHandler);
        }
    };
    document.addEventListener('keydown', escHandler);
}

/**
 * Close the listing detail modal.
 */
function closeListingDetail() {
    const overlay = document.getElementById('listing-modal-overlay');
    if (overlay) overlay.remove();
}

// ============================================================================
// Login
// ============================================================================

async function checkAuth() {
    try {
        const resp = await fetch('/api/auth-check');
        if (resp.ok) {
            const data = await resp.json();
            state.username = data.username || null;
            return true;
        }
        return false;
    } catch {
        return false;
    }
}

async function login() {
    const passwordInput = document.getElementById('login-password');
    const usernameInput = document.getElementById('login-username');
    const errorEl = document.getElementById('login-error');
    if (!passwordInput) return;

    function showError(msg) {
        errorEl.textContent = msg;
        errorEl.classList.remove('hidden');
    }

    const username = (usernameInput?.value || '').trim();
    const password = passwordInput.value;
    if (!username) return showError('Please enter your name.');
    if (!password) return showError('Please enter a password.');

    try {
        const resp = await fetch('/api/login', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ username, password }),
        });

        if (!resp.ok) return showError('Wrong password.');

        const data = await resp.json();
        state.username = data.username || username;
        errorEl.classList.add('hidden');
        showView('upload-view');
    } catch {
        showError('Connection error. Is the server running?');
    }
}

// ============================================================================
// Initialization
// ============================================================================

document.addEventListener('DOMContentLoaded', async () => {
    // Check auth first
    const authed = await checkAuth();
    if (authed) {
        showView('upload-view');
    } else {
        showView('login-view');
    }

    // Login handlers
    document.getElementById('login-btn')?.addEventListener('click', login);
    document.getElementById('login-username')?.addEventListener('keydown', (e) => {
        if (e.key === 'Enter') { e.preventDefault(); document.getElementById('login-password')?.focus(); }
    });
    document.getElementById('login-password')?.addEventListener('keydown', (e) => {
        if (e.key === 'Enter') { e.preventDefault(); login(); }
    });

    // Initialize upload handlers
    initUpload();

    // Explore button: upload files then start discovery
    const exploreBtn = document.getElementById('explore-btn');
    if (exploreBtn) {
        exploreBtn.addEventListener('click', () => {
            uploadFiles();
        });
    }

    // Paste explore button
    document.getElementById('paste-explore-btn')?.addEventListener('click', pasteAndExplore);

    // Demo button: open picker modal
    document.getElementById('demo-btn')?.addEventListener('click', openDemoPicker);

    // Send button in chat
    const sendBtn = document.getElementById('send-btn');
    if (sendBtn) {
        sendBtn.addEventListener('click', () => {
            sendMessage();
        });
    }

    // Chat input: Enter to send (Shift+Enter for newline)
    const chatInput = document.getElementById('chat-input');
    if (chatInput) {
        chatInput.addEventListener('keydown', (e) => {
            if (e.key === 'Enter' && !e.shiftKey) {
                e.preventDefault();
                sendMessage();
            }
        });
    }

    // Download buttons
    const downloadZipBtn = document.getElementById('download-zip-btn');
    if (downloadZipBtn) {
        downloadZipBtn.addEventListener('click', () => {
            downloadResults();
        });
    }

    // Back to chat button (from execution view)
    const backToChatBtn = document.getElementById('back-to-chat-btn');
    if (backToChatBtn) {
        backToChatBtn.addEventListener('click', () => {
            showView('chat-view');
        });
    }
});
