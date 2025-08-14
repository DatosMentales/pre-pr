<script lang="ts">
  import { open as openDialog } from '@tauri-apps/plugin-dialog';
  
  import { invoke } from '@tauri-apps/api/core';

  let selectedFiles = $state<string[]>([]);
  let standardsFile = $state<string | null>(null);
  let exampleFile = $state<string | null>(null);
  let llmProvider = $state<'openai' | 'anthropic' | 'google' | 'local'>('openai');
  let apiKey = $state('');
  let model = $state('');
  let outputLanguage = $state('english');
  let evaluationResult = $state('');
  let evaluationError = $state<string | null>(null);
  let isLoading = $state(false);
  let localModels = $state<string[]>([]);
  let darkMode = $state(false); // State for dark mode

  async function selectFiles() {
    const result = await openDialog({
      multiple: true,
    });
    if (Array.isArray(result)) {
      selectedFiles = result;
    } else if (result) {
      selectedFiles = [result];
    }
  }

  async function selectStandardsFile() {
    const result = await openDialog({
      multiple: false,
      filters: [{
        name: 'Standards',
        extensions: ['txt', 'json', 'md']
      }]
    });
    if (typeof result === 'string') {
      standardsFile = result;
    }
  }

  async function selectExampleFile() {
    const result = await openDialog({
      multiple: false,
      filters: [{
        name: 'Example File',
        extensions: ['txt', 'rs', 'js', 'ts', 'py', 'java', 'cpp', 'h', 'c', 'cs', 'go', 'rb', 'php', 'html', 'css', 'json', 'xml', 'md']
      }]
    });
    if (typeof result === 'string') {
      exampleFile = result;
    }
  }

  async function fetchLocalModels() {
    try {
      isLoading = true;
      const result = await invoke('get_local_llm_models');
      localModels = result as string[];
      if (localModels.length > 0) {
        model = localModels[0]; // Select the first model by default
      } else {
        model = '';
      }
    } catch (error) {
      console.error("Error fetching local models:", error);
      localModels = [];
      model = '';
      alert(`Failed to fetch local models: ${error}`);
    } finally {
      isLoading = false;
    }
  }

  // Fetch local models when the provider changes to 'local'
  $effect(() => {
    if (llmProvider === 'local') {
      fetchLocalModels();
    }
  });

  // Toggle dark mode
  function toggleDarkMode() {
    darkMode = !darkMode;
  }

  $effect(() => {
    document.documentElement.classList.toggle('dark', darkMode);
  });

  async function saveConfig() {
    try {
      await invoke('save_llm_config', {
        llmProvider: llmProvider,
        apiKey: apiKey,
        model: model,
      });
      alert('Configuration saved successfully!');
    } catch (error) {
      alert(`Failed to save configuration: ${error}`);
    }
  }

  async function loadConfig() {
    try {
      const config = await invoke('load_llm_config');
      llmProvider = config.llm_provider as typeof llmProvider;
      apiKey = config.api_key as string;
      model = config.model as string;
      alert('Configuration loaded successfully!');
    } catch (error) {
      alert(`Failed to load configuration: ${error}`);
    }
  }

  function clearAll() {
    selectedFiles = [];
    standardsFile = null;
    exampleFile = null;
    evaluationResult = '';
    evaluationError = null;
  }

  async function copyToClipboard() {
    if (evaluationResult) {
      try {
        await navigator.clipboard.writeText(evaluationResult);
        alert('Results copied to clipboard!');
      } catch (err) {
        alert('Failed to copy results.');
      }
    }
  }

  async function runEvaluation() {
    if (selectedFiles.length === 0 || !standardsFile) {
      alert('Please select source files and a standards file.');
      return;
    }

    if (llmProvider !== 'local' && !apiKey) {
      alert('Please enter an API Key.');
      return;
    }

    if (!model) {
      alert('Please select or enter a model name.');
      return;
    }

    isLoading = true;
    evaluationResult = '';
    evaluationError = null;

    try {
      const result = await invoke('analyze_code', {
        filePaths: selectedFiles,
        standardsPath: standardsFile,
        exampleFilePath: exampleFile,
        llmProvider: llmProvider,
        apiKey: apiKey,
        model: model,
        outputLanguage: outputLanguage,
      });
      if (typeof result === 'object' && result !== null && 'Ok' in result) {
        evaluationResult = result.Ok as string;
      } else if (typeof result === 'object' && result !== null && 'Err' in result) {
        evaluationError = result.Err as string;
      } else {
        // Fallback for unexpected formats
        evaluationResult = result as string;
      }
    } catch (error) {
      evaluationError = `Error: ${error}`;
    } finally {
      isLoading = false;
    }
  }
</script>

<main class="min-h-screen bg-gradient-to-br from-background to-gray-200 dark:from-background-dark dark:to-gray-900 p-8 flex items-center justify-center transition-colors duration-500">
  <div class="bg-white/80 dark:bg-card-dark/80 backdrop-blur-lg rounded-2xl shadow-2xl p-8 w-full max-w-7xl lg:max-w-full xl:max-w-screen-xl 2xl:max-w-screen-2xl transition-all duration-500">
    <div class="flex justify-between items-center mb-10">
      <h1 class="text-5xl font-bold text-transparent bg-clip-text bg-gradient-to-r from-primary to-secondary dark:from-primary-dark dark:to-secondary-dark text-center flex-grow">
        PR Standards Evaluator
      </h1>
      <button onclick={toggleDarkMode} class="p-3 rounded-full bg-gray-200/50 dark:bg-gray-700/50 text-foreground dark:text-foreground-dark hover:bg-gray-300/70 dark:hover:bg-gray-600/70 focus:outline-none focus:ring-4 focus:ring-primary/50 dark:focus:ring-primary-dark/50 transition-all duration-300">
        <div class="w-6 h-6 relative">
          <!-- Sun icon -->
          <svg class="h-6 w-6 absolute transition-all duration-300 transform {darkMode ? 'opacity-0 rotate-90' : 'opacity-100 rotate-0'}" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 3v1m0 16v1m9-9h1M3 12H2m15.325-4.775l-.707-.707M6.343 6.343l-.707-.707m12.728 12.728l-.707-.707M6.343 17.657l-.707-.707M16 12a4 4 0 11-8 0 4 4 0 018 0z" />
          </svg>
          <!-- Moon icon -->
          <svg class="h-6 w-6 absolute transition-all duration-300 transform {darkMode ? 'opacity-100 rotate-0' : 'opacity-0 -rotate-90'}" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M20.354 15.354A9 9 0 018.646 3.646 9.003 9.003 0 0012 21a9.003 9.003 0 008.354-5.646z" />
          </svg>
        </div>
      </button>
    </div>

    <div class="grid grid-cols-1 md:grid-cols-2 gap-12">
      <!-- Left Column: Configuration -->
      <div class="space-y-8">
        <!-- Step 1: Source Files -->
        <div class="p-6 bg-card/50 dark:bg-card-dark/50 rounded-xl shadow-md transition-all duration-300">
          <h2 class="flex items-center text-2xl font-semibold text-foreground dark:text-foreground-dark mb-4">
            <span class="flex items-center justify-center w-8 h-8 mr-4 bg-primary/20 text-primary dark:bg-primary-dark/20 dark:text-primary-dark rounded-full text-lg">1</span>
            Select Source Files
          </h2>
          <button onclick={selectFiles} class="w-full flex items-center justify-center bg-primary/10 hover:bg-primary/20 dark:bg-primary-dark/10 dark:hover:bg-primary-dark/20 text-primary dark:text-primary-dark font-bold py-3 px-6 rounded-lg transition-all duration-300 ease-in-out transform hover:scale-105 focus:outline-none focus:ring-2 focus:ring-primary/50 dark:focus:ring-primary-dark/50">
            <svg class="w-6 h-6 mr-3" fill="none" stroke="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z"></path></svg>
            {selectedFiles.length > 0 ? `${selectedFiles.length} files selected` : 'Select Files'}
          </button>
          {#if selectedFiles.length > 0}
          <ul class="mt-4 space-y-2 text-muted dark:text-muted-dark text-sm max-h-32 overflow-y-auto pr-2">
            {#each selectedFiles as file}
              <li class="bg-background dark:bg-background-dark p-2 rounded-md truncate transition-colors duration-300 text-sm font-mono">{file.split(/[/\\]/).pop()}</li>
            {/each}
          </ul>
          {/if}
        </div>

        <!-- Step 2: Standards File -->
        <div class="p-6 bg-card/50 dark:bg-card-dark/50 rounded-xl shadow-md transition-all duration-300">
          <h2 class="flex items-center text-2xl font-semibold text-foreground dark:text-foreground-dark mb-4">
            <span class="flex items-center justify-center w-8 h-8 mr-4 bg-primary/20 text-primary dark:bg-primary-dark/20 dark:text-primary-dark rounded-full text-lg">2</span>
            Select Standards File
          </h2>
          <button onclick={selectStandardsFile} class="w-full flex items-center justify-center bg-primary/10 hover:bg-primary/20 dark:bg-primary-dark/10 dark:hover:bg-primary-dark/20 text-primary dark:text-primary-dark font-bold py-3 px-6 rounded-lg transition-all duration-300 ease-in-out transform hover:scale-105 focus:outline-none focus:ring-2 focus:ring-primary/50 dark:focus:ring-primary-dark/50">
            <svg class="w-6 h-6 mr-3" fill="none" stroke="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z"></path></svg>
            {standardsFile ? standardsFile.split(/[/\\]/).pop() : 'Select Standards (.txt, .json, .md)'}
          </button>
        </div>

        <!-- Step 3: Example File -->
        <div class="p-6 bg-card/50 dark:bg-card-dark/50 rounded-xl shadow-md transition-all duration-300">
          <h2 class="flex items-center text-2xl font-semibold text-foreground dark:text-foreground-dark mb-4">
            <span class="flex items-center justify-center w-8 h-8 mr-4 bg-primary/20 text-primary dark:bg-primary-dark/20 dark:text-primary-dark rounded-full text-lg">3</span>
            Select Example File (Optional)
          </h2>
          <button onclick={selectExampleFile} class="w-full flex items-center justify-center bg-primary/10 hover:bg-primary/20 dark:bg-primary-dark/10 dark:hover:bg-primary-dark/20 text-primary dark:text-primary-dark font-bold py-3 px-6 rounded-lg transition-all duration-300 ease-in-out transform hover:scale-105 focus:outline-none focus:ring-2 focus:ring-primary/50 dark:focus:ring-primary-dark/50">
            <svg class="w-6 h-6 mr-3" fill="none" stroke="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M10 20l4-16m4 4l4 4-4 4M6 16l-4-4 4-4"></path></svg>
            {exampleFile ? exampleFile.split(/[/\\]/).pop() : 'Select Example File'}
          </button>
        </div>

        <!-- Step 4: LLM Config -->
        <div class="p-6 bg-card/50 dark:bg-card-dark/50 rounded-xl shadow-md transition-all duration-300">
          <h2 class="flex items-center text-2xl font-semibold text-foreground dark:text-foreground-dark mb-4">
            <span class="flex items-center justify-center w-8 h-8 mr-4 bg-primary/20 text-primary dark:bg-primary-dark/20 dark:text-primary-dark rounded-full text-lg">4</span>
            Configure LLM
          </h2>
          <div class="space-y-4">
            <div>
              <label class="block text-muted dark:text-muted-dark text-sm font-bold mb-2" for="llm-provider">LLM Provider</label>
              <div class="relative">
                <select id="llm-provider" bind:value={llmProvider} class="block appearance-none w-full bg-background dark:bg-background-dark border border-gray-300 dark:border-gray-600 text-foreground dark:text-foreground-dark py-3 px-4 pr-8 rounded-lg leading-tight focus:outline-none focus:bg-white dark:focus:bg-gray-800 focus:border-primary dark:focus:border-primary-dark shadow-sm transition-all duration-300">
                  <option value="openai">OpenAI</option>
                  <option value="anthropic">Anthropic</option>
                  <option value="google">Google (Gemini)</option>
                  <option value="local">Local (Ollama/LMStudio)</option>
                </select>
                <div class="pointer-events-none absolute inset-y-0 right-0 flex items-center px-2 text-muted dark:text-muted-dark">
                  <svg class="fill-current h-4 w-4" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 20 20"><path d="M9.293 12.95l.707.707L15.657 8l-1.414-1.414L10 10.828 5.757 6.586 4.343 8z"/></svg>
                </div>
              </div>
            </div>

            {#if llmProvider !== 'local'}
              <div>
                <label class="block text-muted dark:text-muted-dark text-sm font-bold mb-2" for="api-key">API Key</label>
                <input id="api-key" type="password" bind:value={apiKey} placeholder="Enter your API key" class="shadow-sm appearance-none border border-gray-300 dark:border-gray-600 rounded-lg w-full py-3 px-4 text-foreground dark:text-foreground-dark bg-background dark:bg-background-dark leading-tight focus:outline-none focus:ring-2 focus:ring-primary/50 dark:focus:ring-primary-dark/50 focus:border-transparent transition-all duration-300" />
              </div>
              <div>
                <label class="block text-muted dark:text-muted-dark text-sm font-bold mb-2" for="model">Model Name</label>
                <input id="model" type="text" bind:value={model} placeholder="e.g., gpt-4, claude-2" class="shadow-sm appearance-none border border-gray-300 dark:border-gray-600 rounded-lg w-full py-3 px-4 text-foreground dark:text-foreground-dark bg-background dark:bg-background-dark leading-tight focus:outline-none focus:ring-2 focus:ring-primary/50 dark:focus:ring-primary-dark/50 focus:border-transparent transition-all duration-300" />
              </div>
            {:else}
              <div>
                <label class="block text-muted dark:text-muted-dark text-sm font-bold mb-2" for="local-model">Local Model</label>
                <div class="relative">
                  <select id="local-model" bind:value={model} class="block appearance-none w-full bg-background dark:bg-background-dark border border-gray-300 dark:border-gray-600 text-foreground dark:text-foreground-dark py-3 px-4 pr-8 rounded-lg leading-tight focus:outline-none focus:bg-white dark:focus:bg-gray-800 focus:border-primary dark:focus:border-primary-dark shadow-sm transition-all duration-300" disabled={isLoading}>
                    {#each localModels as localModel}
                      <option value={localModel}>{localModel}</option>
                    {:else}
                      <option value="" disabled>No local models found</option>
                    {/each}
                  </select>
                  <div class="pointer-events-none absolute inset-y-0 right-0 flex items-center px-2 text-muted dark:text-muted-dark">
                    <svg class="fill-current h-4 w-4" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 20 20"><path d="M9.293 12.95l.707.707L15.657 8l-1.414-1.414L10 10.828 5.757 6.586 4.343 8z"/></svg>
                  </div>
                </div>
              </div>
            {/if}

             <div>
              <label class="block text-muted dark:text-muted-dark text-sm font-bold mb-2" for="output-language">Output Language</label>
              <div class="relative">
                <select id="output-language" bind:value={outputLanguage} class="block appearance-none w-full bg-background dark:bg-background-dark border border-gray-300 dark:border-gray-600 text-foreground dark:text-foreground-dark py-3 px-4 pr-8 rounded-lg leading-tight focus:outline-none focus:bg-white dark:focus:bg-gray-800 focus:border-primary dark:focus:border-primary-dark shadow-sm transition-all duration-300">
                  <option value="english">English</option>
                  <option value="spanish">Spanish</option>
                </select>
                <div class="pointer-events-none absolute inset-y-0 right-0 flex items-center px-2 text-muted dark:text-muted-dark">
                  <svg class="fill-current h-4 w-4" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 20 20"><path d="M9.293 12.95l.707.707L15.657 8l-1.414-1.414L10 10.828 5.757 6.586 4.343 8z"/></svg>
                </div>
              </div>
            </div>
          </div>
        </div>

        <!-- Action Buttons -->
        <div class="p-6 bg-card/50 dark:bg-card-dark/50 rounded-xl shadow-md transition-all duration-300 space-y-4">
           <div class="flex space-x-4">
            <button onclick={saveConfig} class="flex-1 flex items-center justify-center bg-gray-600/20 hover:bg-gray-600/40 text-foreground dark:text-foreground-dark font-bold py-3 px-6 rounded-lg transition-all duration-300 ease-in-out transform hover:-translate-y-1 focus:outline-none focus:ring-2 focus:ring-gray-500/50">
              <svg class="w-5 h-5 mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M8 7H5a2 2 0 00-2 2v9a2 2 0 002 2h14a2 2 0 002-2V9a2 2 0 00-2-2h-3m-1 4l-3 3m0 0l-3-3m3 3V4"></path></svg>
              Save
            </button>
            <button onclick={loadConfig} class="flex-1 flex items-center justify-center bg-gray-600/20 hover:bg-gray-600/40 text-foreground dark:text-foreground-dark font-bold py-3 px-6 rounded-lg transition-all duration-300 ease-in-out transform hover:-translate-y-1 focus:outline-none focus:ring-2 focus:ring-gray-500/50">
              <svg class="w-5 h-5 mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M8 7H5a2 2 0 00-2 2v9a2 2 0 002 2h14a2 2 0 002-2V9a2 2 0 00-2-2h-3m-1-4l-3-3m0 0l-3 3m3-3v11"></path></svg>
              Load
            </button>
            <button onclick={clearAll} class="flex-1 flex items-center justify-center bg-red-600/20 hover:bg-red-600/40 text-red-600 dark:text-red-500 font-bold py-3 px-6 rounded-lg transition-all duration-300 ease-in-out transform hover:-translate-y-1 focus:outline-none focus:ring-2 focus:ring-red-500/50">
              <svg class="w-5 h-5 mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16"></path></svg>
              Clear
            </button>
          </div>
          <button onclick={runEvaluation} class="w-full text-lg flex items-center justify-center bg-gradient-to-r from-primary to-secondary hover:from-primary-dark hover:to-secondary-dark text-white font-bold py-4 px-6 rounded-lg transition-all duration-300 ease-in-out transform hover:scale-105 hover:shadow-lg focus:outline-none focus:ring-4 focus:ring-primary/50 dark:focus:ring-primary-dark/50" disabled={isLoading}>
            {#if isLoading}
              <svg class="animate-spin -ml-1 mr-3 h-5 w-5 text-white" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
                <circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle>
                <path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
              </svg>
              Processing...
            {:else}
              <svg class="w-6 h-6 mr-3" fill="none" stroke="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M14.752 11.168l-3.197-2.132A1 1 0 0010 9.87v4.263a1 1 0 001.555.832l3.197-2.132a1 1 0 000-1.664z"></path><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M21 12a9 9 0 11-18 0 9 9 0 0118 0z"></path></svg>
              Run Evaluation
            {/if}
          </button>
        </div>
      </div>

      <!-- Right Column: Results -->
      <div class="p-6 bg-card/50 dark:bg-card-dark/50 rounded-xl shadow-md flex flex-col transition-all duration-300">
        <div class="flex justify-between items-center mb-4">
          <h2 class="text-2xl font-semibold text-foreground dark:text-foreground-dark">Evaluation Results</h2>
          <button onclick={copyToClipboard} class="flex items-center justify-center bg-gray-600/20 hover:bg-gray-600/40 text-foreground dark:text-foreground-dark font-bold py-2 px-4 rounded-lg transition-all duration-300 ease-in-out transform hover:-translate-y-1 focus:outline-none focus:ring-2 focus:ring-gray-500/50 w-28">
            {#if copied}
              <svg class="w-5 h-5 mr-2 text-green-500" fill="none" stroke="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z"></path></svg>
              <span>Copied!</span>
            {:else}
              <svg class="w-5 h-5 mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M8 16H6a2 2 0 01-2-2V6a2 2 0 012-2h8a2 2 0 012 2v2m-6 12h8a2 2 0 002-2v-8a2 2 0 00-2-2h-8a2 2 0 00-2 2v8a2 2 0 002 2z"></path></svg>
              <span>Copy</span>
            {/if}
          </button>
        </div>
        <div class="flex-grow bg-background dark:bg-background-dark p-4 rounded-lg border border-gray-200/50 dark:border-gray-700/50 overflow-auto h-[calc(100vh-200px)] transition-colors duration-300 text-foreground dark:text-foreground-dark">
          {#if evaluationError}
            <div class="flex items-center p-4 bg-red-500/10 text-red-500 rounded-lg">
              <svg class="w-6 h-6 mr-3" fill="none" stroke="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 8v4m0 4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z"></path></svg>
              <p>{evaluationError}</p>
            </div>
          {:else if evaluationResult}
            <div class="prose dark:prose-invert max-w-none space-y-1 font-mono text-sm">
              {#each evaluationResult.split('\n') as line}
                {#if line.startsWith('-')}
                  <div class="diff-removed">{line}</div>
                {:else if line.startsWith('+')}
                  <div class="diff-added">{line}</div>
                {:else if line.startsWith('!')}
                  <div class="diff-comment">{line}</div>
                {:else}
                  <div>{line}</div>
                {/if}
              {/each}
            </div>
          {:else}
            <div class="flex flex-col items-center justify-center h-full text-muted dark:text-muted-dark">
              <svg class="w-24 h-24 mb-4 text-gray-300 dark:text-gray-600" fill="none" stroke="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="1" d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z"></path></svg>
              <p class="text-lg">Results will be displayed here.</p>
              <p class="text-sm">Run an evaluation to get started.</p>
            </div>
          {/if}
        </div>
      </div>
    </div>
  </div>
</main>

<style lang="postcss">
  .diff-added {
    color: #10B981; /* green-600 */
    display: block;
  }
  .diff-removed {
    color: #EF4444; /* red-600 */
    display: block;
  }
  .diff-comment {
    color: #F59E0B; /* amber-500 (orange) */
    display: block;
  }
</style>