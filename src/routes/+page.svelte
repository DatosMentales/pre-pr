<script lang="ts">
  import { open } from '@tauri-apps/plugin-dialog';
  
  import { invoke } from '@tauri-apps/api/core';
  import { marked } from 'marked';

  let selectedFiles = $state<string[]>([]);
  let standardsFile = $state<string | null>(null);
  let exampleFile = $state<string | null>(null);
  let llmProvider = $state<'openai' | 'anthropic' | 'google' | 'local'>('openai');
  let apiKey = $state('');
  let model = $state('');
  let evaluationResult = $state('');
  let isLoading = $state(false);
  let localModels = $state<string[]>([]);
  let darkMode = $state(false); // State for dark mode

  // Custom Marked renderer
  const renderer = new marked.Renderer();
  renderer.paragraph = (text) => {
    if (text.startsWith('+ ')) {
      return `<p class="diff-added">${text}</p>\n`;
    }
    if (text.startsWith('- ')) {
      return `<p class="diff-removed">${text}</p>\n`;
    }
    if (text.startsWith('! ')) {
      return `<p class="diff-comment">${text}</p>\n`;
    }
    return `<p>${text}</p>\n`;
  };

  let renderedHtml = $derived(marked.parse(evaluationResult, {
    renderer: renderer,
    gfm: true, // Enable GitHub Flavored Markdown
    breaks: true, // Enable GFM line breaks
  })); // Derived state for rendered HTML

  async function selectFiles() {
    const result = await open({
      multiple: true,
    });
    if (Array.isArray(result)) {
      selectedFiles = result;
    } else if (result) {
      selectedFiles = [result];
    }
  }

  async function selectStandardsFile() {
    const result = await open({
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
    const result = await open({
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

    try {
      const result = await invoke('analyze_code', {
        filePaths: selectedFiles,
        standardsPath: standardsFile,
        llmProvider: llmProvider,
        apiKey: apiKey,
        model: model,
      });
      evaluationResult = result as string;
    } catch (error) {
      evaluationResult = `<p class="text-red-500">Error: ${error}</p>`;
    } finally {
      isLoading = false;
    }
  }
</script>

<main class="min-h-screen bg-gray-100 dark:bg-gray-900 p-8 flex items-center justify-center transition-colors duration-300">
  <div class="bg-white dark:bg-gray-800 rounded-lg shadow-xl p-8 w-full max-w-7xl lg:max-w-full xl:max-w-screen-xl 2xl:max-w-screen-2xl transition-colors duration-300">
    <div class="flex justify-between items-center mb-8">
      <h1 class="text-4xl font-extrabold text-gray-800 dark:text-white text-center flex-grow">PR Standards Evaluator</h1>
      <button onclick={toggleDarkMode} class="p-2 rounded-full bg-gray-200 dark:bg-gray-700 text-gray-800 dark:text-white focus:outline-none focus:ring-2 focus:ring-blue-500 transition-colors duration-300">
        {#if darkMode}
          <!-- Sun icon for light mode -->
          <svg xmlns="http://www.w3.org/2000/svg" class="h-6 w-6" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 3v1m0 16v1m9-9h1M3 12H2m15.325-4.775l-.707-.707M6.343 6.343l-.707-.707m12.728 12.728l-.707-.707M6.343 17.657l-.707-.707M16 12a4 4 0 11-8 0 4 4 0 018 0z" />
          </svg>
        {:else}
          <!-- Moon icon for dark mode -->
          <svg xmlns="http://www.w3.org/2000/svg" class="h-6 w-6" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M20.354 15.354A9 9 0 018.646 3.646 9.003 9.003 0 0012 21a9.003 9.003 0 008.354-5.646z" />
          </svg>
        {/if}
      </button>
    </div>

    <div class="grid grid-cols-1 md:grid-cols-2 gap-12">
      <!-- Left Column: Configuration -->
      <div>
        <div class="mb-8 p-6 bg-gray-50 dark:bg-gray-700 rounded-lg shadow-sm transition-colors duration-300">
          <h2 class="text-2xl font-semibold text-gray-700 dark:text-white mb-4">1. Select Source Files</h2>
          <button onclick={selectFiles} class="w-full bg-blue-600 hover:bg-blue-700 text-white font-bold py-3 px-6 rounded-lg transition duration-300 ease-in-out transform hover:scale-105 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:ring-opacity-50">
            {selectedFiles.length > 0 ? `${selectedFiles.length} files selected` : 'Select Files'}
          </button>
          <ul class="mt-4 space-y-2 text-gray-600 dark:text-gray-300 text-sm">
            {#each selectedFiles as file}
              <li class="bg-gray-100 dark:bg-gray-600 p-2 rounded-md truncate transition-colors duration-300">{file}</li>
            {/each}
          </ul>
        </div>

        <div class="mb-8 p-6 bg-gray-50 dark:bg-gray-700 rounded-lg shadow-sm transition-colors duration-300">
          <h2 class="text-2xl font-semibold text-gray-700 dark:text-white mb-4">2. Select Standards File</h2>
          <button onclick={selectStandardsFile} class="w-full bg-blue-600 hover:bg-blue-700 text-white font-bold py-3 px-6 rounded-lg transition duration-300 ease-in-out transform hover:scale-105 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:ring-opacity-50">
            {standardsFile ? standardsFile : 'Select Standards (.txt, .json, .md)'}
          </button>
          {#if standardsFile}
            <p class="mt-4 text-gray-600 dark:text-gray-300 text-sm bg-gray-100 dark:bg-gray-600 p-2 rounded-md truncate transition-colors duration-300">{standardsFile}</p>
          {/if}
        </div>

        <div class="mb-8 p-6 bg-gray-50 dark:bg-gray-700 rounded-lg shadow-sm transition-colors duration-300">
          <h2 class="text-2xl font-semibold text-gray-700 dark:text-white mb-4">3. Select Example File (Optional)</h2>
          <button onclick={selectExampleFile} class="w-full bg-blue-600 hover:bg-blue-700 text-white font-bold py-3 px-6 rounded-lg transition duration-300 ease-in-out transform hover:scale-105 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:ring-opacity-50">
            {exampleFile ? exampleFile : 'Select Example File'}
          </button>
          {#if exampleFile}
            <p class="mt-4 text-gray-600 dark:text-gray-300 text-sm bg-gray-100 dark:bg-gray-600 p-2 rounded-md truncate transition-colors duration-300">{exampleFile}</p>
          {/if}
        </div>

        <div class="mb-8 p-6 bg-gray-50 dark:bg-gray-700 rounded-lg shadow-sm transition-colors duration-300">
          <h2 class="text-2xl font-semibold text-gray-700 dark:text-white mb-4">3. Configure LLM</h2>
          <div class="mb-4">
            <label class="block text-gray-700 dark:text-gray-200 text-sm font-bold mb-2" for="llm-provider">
              LLM Provider
            </label>
            <div class="relative">
              <select id="llm-provider" bind:value={llmProvider} class="block appearance-none w-full bg-white dark:bg-gray-600 border border-gray-300 dark:border-gray-500 text-gray-700 dark:text-white py-3 px-4 pr-8 rounded-lg leading-tight focus:outline-none focus:bg-white dark:focus:bg-gray-600 focus:border-blue-500 shadow-sm transition-colors duration-300">
                <option value="openai">OpenAI</option>
                <option value="anthropic">Anthropic</option>
                <option value="google">Google (Gemini)</option>
                <option value="local">Local (Ollama/LMStudio)</option>
              </select>
              <div class="pointer-events-none absolute inset-y-0 right-0 flex items-center px-2 text-gray-700 dark:text-gray-200">
                <svg class="fill-current h-4 w-4" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 20 20"><path d="M9.293 12.95l.707.707L15.657 8l-1.414-1.414L10 10.828 5.757 6.586 4.343 8z"/></svg>
              </div>
            </div>
          </div>

          {#if llmProvider !== 'local'}
            <div class="mb-4">
              <label class="block text-gray-700 dark:text-gray-200 text-sm font-bold mb-2" for="api-key">
                API Key
              </label>
              <input id="api-key" type="password" bind:value={apiKey} placeholder="Enter your API key" class="shadow-sm appearance-none border rounded-lg w-full py-3 px-4 text-gray-700 dark:text-white dark:bg-gray-600 dark:border-gray-500 leading-tight focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent transition-colors duration-300" />
            </div>
            <div class="mb-4">
              <label class="block text-gray-700 dark:text-gray-200 text-sm font-bold mb-2" for="model">
                Model Name
              </label>
              <input id="model" type="text" bind:value={model} placeholder="e.g., gpt-4, claude-2" class="shadow-sm appearance-none border rounded-lg w-full py-3 px-4 text-gray-700 dark:text-white dark:bg-gray-600 dark:border-gray-500 leading-tight focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent transition-colors duration-300" />
            </div>
          {:else}
            <div class="mb-4">
              <label class="block text-gray-700 dark:text-gray-200 text-sm font-bold mb-2" for="local-model">
                Local Model
              </label>
              <div class="relative">
                <select id="local-model" bind:value={model} class="block appearance-none w-full bg-white dark:bg-gray-600 border border-gray-300 dark:border-gray-500 text-gray-700 dark:text-white py-3 px-4 pr-8 rounded-lg leading-tight focus:outline-none focus:bg-white dark:focus:bg-gray-600 focus:border-blue-500 shadow-sm transition-colors duration-300" disabled={isLoading}>
                  {#each localModels as localModel}
                    <option value={localModel}>{localModel}</option>
                  {:else}
                    <option value="" disabled>No local models found</option>
                  {/each}
                </select>
                <div class="pointer-events-none absolute inset-y-0 right-0 flex items-center px-2 text-gray-700 dark:text-gray-200">
                  <svg class="fill-current h-4 w-4" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 20 20"><path d="M9.293 12.95l.707.707L15.657 8l-1.414-1.414L10 10.828 5.757 6.586 4.343 8z"/></svg>
                </div>
              </div>
            </div>
          {/if}

          <div class="flex space-x-4 mt-6">
            <button onclick={saveConfig} class="flex-1 bg-gray-600 hover:bg-gray-700 text-white font-bold py-3 px-6 rounded-lg transition duration-300 ease-in-out transform hover:scale-105 focus:outline-none focus:ring-2 focus:ring-gray-500 focus:ring-opacity-50">
              Save Config
            </button>
            <button onclick={loadConfig} class="flex-1 bg-gray-600 hover:bg-gray-700 text-white font-bold py-3 px-6 rounded-lg transition duration-300 ease-in-out transform hover:scale-105 focus:outline-none focus:ring-2 focus:ring-gray-500 focus:ring-opacity-50">
              Load Config
            </button>
          </div>

          <button onclick={runEvaluation} class="w-full bg-green-600 hover:bg-green-700 text-white font-bold py-3 px-6 rounded-lg transition duration-300 ease-in-out transform hover:scale-105 focus:outline-none focus:ring-2 focus:ring-green-500 focus:ring-opacity-50 mt-4" disabled={isLoading}>
            {#if isLoading}
              <span class="inline-block w-5 h-5 border-2 border-t-2 border-white rounded-full animate-spin mr-2"></span>
            {/if}
            Run Evaluation
          </button>
        </div>
      </div>

      <!-- Right Column: Results -->
      <div class="p-6 bg-gray-50 dark:bg-gray-700 rounded-lg shadow-sm flex flex-col transition-colors duration-300 text-white">
        <h2 class="text-2xl font-semibold text-gray-700 dark:text-white mb-4">Evaluation Results</h2>
        <div class="flex-grow bg-white dark:bg-gray-900 p-4 rounded-lg border border-gray-200 dark:border-gray-600 overflow-auto h-[calc(100vh-200px)] transition-colors duration-300">
          {#if evaluationResult}
            <div class="prose dark:prose-invert max-w-none">
              {@html renderedHtml}
            </div>
          {:else}
            <p class="text-gray-500 dark:text-gray-400">Results will be displayed here.</p>
          {/if}
        </div>
      </div>
    </div>
  </div>
</main>

<style lang="postcss">
  .diff-added {
    color: #10B981; /* green-600 */
  }
  .diff-removed {
    color: #EF4444; /* red-600 */
  }
  .diff-comment {
    color: #3B82F6; /* blue-600 */
  }
</style>