/**
 * ListingAgent — Frontend Application
 *
 * State-machine driven single-page app.
 * States: UPLOAD -> DISCOVER -> INTERVIEW -> RECIPE_TEST -> EXECUTING -> RESULTS
 *
 * No framework. Vanilla JS. Chat is the primary interface for phases 1-3.
 */

// ============================================================================
// Global State
// ============================================================================

const state = {
    phase: 'UPLOAD',
    jobId: null,
    conversationHistory: [],
    dataModel: null,
    styleProfile: null,
    recipe: null,
    testResults: [],
    batchResults: [],
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
function formatMessage(text) {
    if (!text) return '';
    if (typeof marked !== 'undefined') {
        return marked.parse(text);
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
 * Initialize drag-and-drop and file input handlers.
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
        showToast(`${state.uploadedFiles.length} files uploaded successfully.`, 'success');

        // Transition to discovery
        await startDiscovery();
    } catch (error) {
        hideLoading();
        showToast('Upload failed: ' + error.message, 'error');
    }
}

/**
 * Load demo dataset and start discovery.
 */
async function loadDemo() {
    showLoading('Loading sample data...');

    try {
        const result = await api('/api/load-demo', { method: 'POST' });

        state.jobId = result.job_id;
        showToast(`Demo loaded: ${result.file_count} files.`, 'success');

        await startDiscovery();
    } catch (error) {
        hideLoading();
        showToast('Failed to load demo: ' + error.message, 'error');
    }
}

/**
 * Trigger Phase 1 discovery.
 */
async function startDiscovery() {
    state.phase = 'DISCOVER';
    showView('chat-view');
    updatePhaseIndicator('DISCOVER');
    addPhaseBanner('DISCOVER');
    showLoading('Analyzing your data...');

    // Disable chat input during discovery
    setChatInputEnabled(false);

    try {
        const result = await api('/api/discover', {
            method: 'POST',
            body: JSON.stringify({ job_id: state.jobId }),
        });

        hideLoading();

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

        // Enable chat for user to confirm/correct data mapping
        setChatInputEnabled(true);

        // Show the "Confirm data mapping" action button
        showActionButton('confirm-mapping-btn', 'Confirm Data Mapping', async () => {
            hideActionButton('confirm-mapping-btn');
            await buildDataModel();
        });

    } catch (error) {
        hideLoading();
        setChatInputEnabled(true);
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
function addMessage(role, content) {
    const chatMessages = document.getElementById('chat-messages');
    if (!chatMessages) return;

    const wrapper = document.createElement('div');
    wrapper.className = `chat-message chat-message-${role}`;

    const bubble = document.createElement('div');
    bubble.className = 'chat-bubble';
    bubble.innerHTML = formatMessage(content);

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
            title: 'Data Mapping',
            description: 'The AI is analyzing your files to understand what products you have and how images connect to data.',
        },
        INTERVIEW: {
            step: '2',
            title: 'Brand Profile',
            description: 'Answer a few questions about your selling style so listings match your brand voice.',
        },
        RECIPE_TEST: {
            step: '3',
            title: 'Listing Template',
            description: 'The AI will draft a listing formula and test it on a few products. Review and refine until you\'re happy.',
        },
    };

    const b = banners[phase];
    if (!b) return;

    const wrapper = document.createElement('div');
    wrapper.className = 'flex justify-center my-5';
    wrapper.innerHTML = `
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

    // Check if the profile is ready or backend says to start recipe
    if (result.phase === 'profile_ready' || result.phase === 'start_recipe') {
        if (result.style_profile) {
            state.styleProfile = result.style_profile;
            updateContextPanel();
        }

        addSystemMessage('Style profile complete. Building your listing recipe...');
        await startRecipeBuilding();
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
    showLoading('Refining recipe based on your feedback...');

    try {
        const result = await api('/api/chat', {
            method: 'POST',
            body: JSON.stringify({
                job_id: state.jobId,
                message: message,
                conversation_history: state.conversationHistory,
            }),
        });

        hideLoading();

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
        hideLoading();
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
    showLoading('Building data model...');
    setChatInputEnabled(false);

    try {
        const result = await api('/api/build-data-model', {
            method: 'POST',
            body: JSON.stringify({
                job_id: state.jobId,
                conversation_history: state.conversationHistory,
            }),
        });

        state.dataModel = result.data_model;
        hideLoading();

        const productCount = (state.dataModel.products || []).length;
        addSystemMessage(`Data model built: ${productCount} products mapped.`);

        updateContextPanel();

        // Transition to interview
        await startInterview();

    } catch (error) {
        hideLoading();
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
async function startInterview() {
    state.phase = 'INTERVIEW';
    updatePhaseIndicator('INTERVIEW');
    addPhaseBanner('INTERVIEW');

    showLoading('Starting style interview...');
    setChatInputEnabled(false);

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

        hideLoading();

        addMessage('assistant', result.response);
        state.conversationHistory.push({
            role: 'assistant',
            content: result.response,
        });

        setChatInputEnabled(true);

    } catch (error) {
        hideLoading();
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

    showLoading('Drafting your listing recipe...');
    setChatInputEnabled(false);

    try {
        // The backend test-recipe endpoint drafts (if not already drafted) then tests
        const result = await api('/api/test-recipe', {
            method: 'POST',
            body: JSON.stringify({ job_id: state.jobId }),
        });

        hideLoading();

        state.recipe = result.recipe;
        state.testResults = result.test_results || [];

        // Announce in chat
        addMessage('assistant', buildRecipeTestSummary(state.testResults));

        state.conversationHistory.push({
            role: 'assistant',
            content: 'Recipe drafted and tested on sample products. See the results in the panel.',
        });

        updateContextPanel();
        setChatInputEnabled(true);

        // Show action buttons
        showActionButton('approve-recipe-btn', 'Approve Recipe', async () => {
            await approveRecipe();
        });

        showActionButton('test-recipe-btn', 'Re-test Recipe', async () => {
            await testRecipe();
        });

    } catch (error) {
        hideLoading();
        setChatInputEnabled(true);
        showToast('Recipe building failed: ' + error.message, 'error');
        addMessage('assistant', 'I had trouble creating the recipe. Could you describe what kind of listing format you prefer?');
    }
}

/**
 * Build a chat-friendly summary of test results.
 */
function buildRecipeTestSummary(testResults) {
    if (!testResults || testResults.length === 0) {
        return 'I drafted a recipe but could not test it on any samples. You can approve it or provide feedback.';
    }

    const lines = ['Here are the test results for the recipe:\n'];

    testResults.forEach((tr, i) => {
        const name = tr.product_name || tr.product_id || `Sample ${i + 1}`;
        const score = tr.validation?.score ?? '?';
        const passed = tr.validation?.passed ? 'Passed' : 'Needs work';
        const issues = tr.validation?.issues || [];
        const title = tr.listing?.title || '(no title generated)';

        lines.push(`**${name}** - Score: ${score}/100 (${passed})`);
        lines.push(`  Title: "${title}"`);

        if (issues.length > 0) {
            lines.push(`  Issues: ${issues.join(', ')}`);
        }
        lines.push('');
    });

    const avgScore = testResults.reduce((sum, tr) => sum + (tr.validation?.score || 0), 0) / testResults.length;
    lines.push(`**Average score: ${Math.round(avgScore)}/100**`);
    lines.push('\nReview the results in the right panel. You can approve the recipe or give me feedback to refine it.');

    return lines.join('\n');
}

/**
 * Test (or re-test) the current recipe.
 */
async function testRecipe() {
    showLoading('Testing recipe on sample products...');
    hideActionButton('test-recipe-btn');
    hideActionButton('approve-recipe-btn');

    try {
        const result = await api('/api/test-recipe', {
            method: 'POST',
            body: JSON.stringify({ job_id: state.jobId }),
        });

        hideLoading();

        state.recipe = result.recipe;
        state.testResults = result.test_results || [];

        addMessage('assistant', buildRecipeTestSummary(state.testResults));
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
        hideLoading();
        showToast('Testing failed: ' + error.message, 'error');
    }
}

/**
 * Approve the recipe and transition to execution.
 */
async function approveRecipe() {
    showLoading('Locking recipe...');
    hideActionButton('approve-recipe-btn');
    hideActionButton('test-recipe-btn');

    try {
        const result = await api('/api/approve-recipe', {
            method: 'POST',
            body: JSON.stringify({ job_id: state.jobId }),
        });

        state.recipe = result.recipe;
        hideLoading();

        addSystemMessage('Recipe approved! Starting batch execution...');

        // Transition to execution
        await startExecution();

    } catch (error) {
        hideLoading();
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
    if (statsContainer) statsContainer.classList.add('hidden');

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

    // Update progress bar
    const progressBar = document.getElementById('progress-bar');
    const progressText = document.getElementById('progress-text');

    if (progressBar && total > 0) {
        const pct = Math.round((completed / total) * 100);
        progressBar.style.width = `${pct}%`;
    }

    if (progressText) {
        progressText.textContent = `${completed} / ${total} products`;
    }

    // Add a listing card to the grid
    addListingCard({
        product_id,
        title: title || product_id,
        score,
        status,
    });

    // Store result
    state.batchResults.push(data);
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

    // Show stats
    showExecutionStats(data.report);

    // Show download section
    const downloadSection = document.getElementById('download-section');
    if (downloadSection) downloadSection.classList.remove('hidden');

    showToast('Batch execution complete!', 'success');
}

/**
 * Display execution statistics.
 */
function showExecutionStats(report) {
    const container = document.getElementById('execution-stats');
    if (!container || !report) return;

    container.classList.remove('hidden');
    container.innerHTML = `
        <div class="grid grid-cols-2 sm:grid-cols-4 gap-4">
            <div class="bg-white rounded-lg border border-slate-200 p-4 text-center">
                <div class="text-2xl font-bold text-slate-800">${report.total}</div>
                <div class="text-sm text-slate-500">Total</div>
            </div>
            <div class="bg-white rounded-lg border border-slate-200 p-4 text-center">
                <div class="text-2xl font-bold text-green-600">${report.succeeded}</div>
                <div class="text-sm text-slate-500">Succeeded</div>
            </div>
            <div class="bg-white rounded-lg border border-slate-200 p-4 text-center">
                <div class="text-2xl font-bold text-red-500">${report.failed}</div>
                <div class="text-sm text-slate-500">Failed</div>
            </div>
            <div class="bg-white rounded-lg border border-slate-200 p-4 text-center">
                <div class="text-2xl font-bold text-blue-500">${report.avg_score}</div>
                <div class="text-sm text-slate-500">Avg Score</div>
            </div>
        </div>
        <div class="mt-3 text-sm text-slate-500 text-center">
            ${report.retried} retried | ${report.elapsed_seconds}s elapsed
        </div>
    `;
}

/**
 * Add a listing card to the execution results grid.
 */
function addListingCard(data) {
    const grid = document.getElementById('listings-grid');
    if (!grid) return;

    const card = document.createElement('div');
    card.className = 'listing-card view-enter';

    const scoreColor = data.score >= 80
        ? 'text-green-600 bg-green-50'
        : data.score >= 50
            ? 'text-amber-600 bg-amber-50'
            : 'text-red-600 bg-red-50';

    const statusIcon = data.status === 'ok'
        ? '<svg class="w-4 h-4 text-green-500" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 13l4 4L19 7"/></svg>'
        : '<svg class="w-4 h-4 text-red-500" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"/></svg>';

    card.innerHTML = `
        <div class="flex items-start justify-between mb-2">
            <div class="flex items-center gap-2">
                ${statusIcon}
                <span class="text-xs text-slate-400 font-mono">${data.product_id}</span>
            </div>
            <span class="text-xs font-semibold px-2 py-0.5 rounded-full ${scoreColor}">
                ${data.score !== null && data.score !== undefined ? data.score + '/100' : '--'}
            </span>
        </div>
        <h3 class="listing-card-title text-sm">${escapeHtml(data.title || 'Untitled')}</h3>
    `;

    grid.appendChild(card);

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
 * Render data model info in context panel.
 */
function renderDataModelSection() {
    if (!state.dataModel) return '';

    const products = state.dataModel.products || [];
    const unmatched = state.dataModel.unmatched_images || [];

    let productsHtml = '';
    const displayProducts = products.slice(0, 8);
    displayProducts.forEach(p => {
        const name = p.name || p.sku || p.id;
        const category = p.category ? ` (${p.category})` : '';
        const price = p.price ? ` - $${p.price}` : '';
        const hasImage = p.image_files && p.image_files.length > 0;
        const imageIcon = hasImage
            ? '<svg class="w-3 h-3 text-green-500 inline" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 13l4 4L19 7"/></svg>'
            : '<svg class="w-3 h-3 text-slate-300 inline" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 16l4.586-4.586a2 2 0 012.828 0L16 16m-2-2l1.586-1.586a2 2 0 012.828 0L20 14m-6-6h.01M6 20h12a2 2 0 002-2V6a2 2 0 00-2-2H6a2 2 0 00-2 2v12a2 2 0 002 2z"/></svg>';

        productsHtml += `
            <div class="context-item flex items-center justify-between">
                <div class="flex items-center gap-2 min-w-0">
                    ${imageIcon}
                    <span class="text-sm truncate">${escapeHtml(name)}${category}</span>
                </div>
                <span class="text-xs text-slate-400 whitespace-nowrap">${price}</span>
            </div>
        `;
    });

    if (products.length > 8) {
        productsHtml += `<div class="text-xs text-slate-400 text-center py-2">+ ${products.length - 8} more products</div>`;
    }

    let unmatchedHtml = '';
    if (unmatched.length > 0) {
        unmatchedHtml = `
            <div class="context-item bg-amber-50 border-amber-200">
                <span class="text-sm text-amber-700">${unmatched.length} unmatched image${unmatched.length !== 1 ? 's' : ''}</span>
            </div>
        `;
    }

    return `
        <div class="context-section">
            <div class="context-section-title">Products (${products.length})</div>
            ${productsHtml}
            ${unmatchedHtml}
        </div>
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
        const issues = tr.validation?.issues || [];
        const listing = tr.listing || {};

        const scoreColor = score >= 80 ? 'bg-green-100 text-green-700'
            : score >= 50 ? 'bg-amber-100 text-amber-700'
            : 'bg-red-100 text-red-700';

        const statusBadge = passed
            ? '<span class="badge badge-success text-xs">Passed</span>'
            : '<span class="badge badge-error text-xs">Needs work</span>';

        let issuesHtml = '';
        if (issues.length > 0) {
            issuesHtml = `
                <div class="mt-2 space-y-1">
                    ${issues.map(i => `<div class="text-xs text-red-500">- ${escapeHtml(i)}</div>`).join('')}
                </div>
            `;
        }

        cardsHtml += `
            <div class="context-item">
                <div class="flex items-center justify-between mb-1">
                    <span class="text-sm font-medium truncate">${escapeHtml(name)}</span>
                    <span class="text-xs font-bold px-2 py-0.5 rounded-full ${scoreColor}">${score}/100</span>
                </div>
                ${listing.title ? `<div class="text-xs text-slate-500 truncate mb-1">"${escapeHtml(listing.title)}"</div>` : ''}
                ${statusBadge}
                ${issuesHtml}
            </div>
        `;
    });

    const avg = state.testResults.reduce((sum, tr) => sum + (tr.validation?.score || 0), 0) / state.testResults.length;

    return `
        <div class="context-section">
            <div class="context-section-title">
                Test Results
                <span class="float-right text-xs font-normal normal-case">Avg: ${Math.round(avg)}/100</span>
            </div>
            ${cardsHtml}
        </div>
    `;
}

// ============================================================================
// Download
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

// ============================================================================
// Login
// ============================================================================

async function checkAuth() {
    try {
        const resp = await fetch('/api/auth-check');
        return resp.ok;
    } catch {
        return false;
    }
}

async function login() {
    const input = document.getElementById('login-password');
    const errorEl = document.getElementById('login-error');
    if (!input) return;

    function showError(msg) {
        errorEl.textContent = msg;
        errorEl.classList.remove('hidden');
    }

    const password = input.value;
    if (!password) return showError('Please enter a password.');

    try {
        const resp = await fetch('/api/login', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ password }),
        });

        if (!resp.ok) return showError('Wrong password.');

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

    // Demo button: load sample data and start discovery
    document.getElementById('demo-btn')?.addEventListener('click', loadDemo);

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
