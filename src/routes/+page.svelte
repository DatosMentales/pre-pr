<script lang="ts">
  import { open } from '@tauri-apps/plugin-dialog';
  import { readTextFile } from '@tauri-apps/plugin-fs';
  import { invoke } from '@tauri-apps/api/core';

  let selectedFiles: string[] = [];
  let standardsFile: string | null = null;
  let llmProvider: 'openai' | 'anthropic' | 'local' = 'openai';
  let apiKey = '';
  let model = '';
  let evaluationResult = '';
  let isLoading = false;
  let localModels: string[] = [];

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
  $: if (llmProvider === 'local') {
    fetchLocalModels();
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

<main class="min-h-screen bg-gray-100 p-8 flex items-center justify-center">
  <div class="bg-white rounded-lg shadow-xl p-8 w-full max-w-6xl">
    <h1 class="text-4xl font-extrabold text-gray-800 mb-8 text-center">PR Standards Evaluator</h1>

    <div class="grid grid-cols-1 md:grid-cols-2 gap-12">
      <!-- Left Column: Configuration -->
      <div>
        <div class="mb-8 p-6 bg-gray-50 rounded-lg shadow-sm">
          <h2 class="text-2xl font-semibold text-gray-700 mb-4">1. Select Source Files</h2>
          <button on:click={selectFiles} class="w-full bg-blue-600 hover:bg-blue-700 text-white font-bold py-3 px-6 rounded-lg transition duration-300 ease-in-out transform hover:scale-105 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:ring-opacity-50">
            {selectedFiles.length > 0 ? `${selectedFiles.length} files selected` : 'Select Files'}
          </button>
          <ul class="mt-4 space-y-2 text-gray-600 text-sm">
            {#each selectedFiles as file}
              <li class="bg-gray-100 p-2 rounded-md truncate">{file}</li>
            {/each}
          </ul>
        </div>

        <div class="mb-8 p-6 bg-gray-50 rounded-lg shadow-sm">
          <h2 class="text-2xl font-semibold text-gray-700 mb-4">2. Select Standards File</h2>
          <button on:click={selectStandardsFile} class="w-full bg-blue-600 hover:bg-blue-700 text-white font-bold py-3 px-6 rounded-lg transition duration-300 ease-in-out transform hover:scale-105 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:ring-opacity-50">
            {standardsFile ? standardsFile : 'Select Standards (.txt, .json, .md)'}
          </button>
          {#if standardsFile}
            <p class="mt-4 text-gray-600 text-sm bg-gray-100 p-2 rounded-md truncate">{standardsFile}</p>
          {/if}
        </div>

        <div class="mb-8 p-6 bg-gray-50 rounded-lg shadow-sm">
          <h2 class="text-2xl font-semibold text-gray-700 mb-4">3. Configure LLM</h2>
          <div class="mb-4">
            <label class="block text-gray-700 text-sm font-bold mb-2" for="llm-provider">
              LLM Provider
            </label>
            <div class="relative">
              <select id="llm-provider" bind:value={llmProvider} class="block appearance-none w-full bg-white border border-gray-300 text-gray-700 py-3 px-4 pr-8 rounded-lg leading-tight focus:outline-none focus:bg-white focus:border-blue-500 shadow-sm">
                <option value="openai">OpenAI</option>
                <option value="anthropic">Anthropic</option>
                <option value="local">Local (Ollama/LMStudio)</option>
              </select>
              <div class="pointer-events-none absolute inset-y-0 right-0 flex items-center px-2 text-gray-700">
                <svg class="fill-current h-4 w-4" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 20 20"><path d="M9.293 12.95l.707.707L15.657 8l-1.414-1.414L10 10.828 5.757 6.586 4.343 8z"/></svg>
              </div>
            </div>
          </div>

          {#if llmProvider !== 'local'}
            <div class="mb-4">
              <label class="block text-gray-700 text-sm font-bold mb-2" for="api-key">
                API Key
              </label>
              <input id="api-key" type="password" bind:value={apiKey} placeholder="Enter your API key" class="shadow-sm appearance-none border rounded-lg w-full py-3 px-4 text-gray-700 leading-tight focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent" />
            </div>
            <div class="mb-4">
              <label class="block text-gray-700 text-sm font-bold mb-2" for="model">
                Model Name
              </label>
              <input id="model" type="text" bind:value={model} placeholder="e.g., gpt-4, claude-2" class="shadow-sm appearance-none border rounded-lg w-full py-3 px-4 text-gray-700 leading-tight focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent" />
            </div>
          {:else}
            <div class="mb-4">
              <label class="block text-gray-700 text-sm font-bold mb-2" for="local-model">
                Local Model
              </label>
              <div class="relative">
                <select id="local-model" bind:value={model} class="block appearance-none w-full bg-white border border-gray-300 text-gray-700 py-3 px-4 pr-8 rounded-lg leading-tight focus:outline-none focus:bg-white focus:border-blue-500 shadow-sm" disabled={isLoading}>
                  {#each localModels as localModel}
                    <option value={localModel}>{localModel}</option>
                  {:else}
                    <option value="" disabled>No local models found</option>
                  {/each}
                </select>
                <div class="pointer-events-none absolute inset-y-0 right-0 flex items-center px-2 text-gray-700">
                  <svg class="fill-current h-4 w-4" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 20 20"><path d="M9.293 12.95l.707.707L15.657 8l-1.414-1.414L10 10.828 5.757 6.586 4.343 8z"/></svg>
                </div>
              </div>
            </div>
          {/if}

          <button on:click={runEvaluation} class="w-full bg-green-600 hover:bg-green-700 text-white font-bold py-3 px-6 rounded-lg transition duration-300 ease-in-out transform hover:scale-105 focus:outline-none focus:ring-2 focus:ring-green-500 focus:ring-opacity-50" disabled={isLoading}>
            {#if isLoading}
              <span class="inline-block w-5 h-5 border-2 border-t-2 border-white rounded-full animate-spin mr-2"></span>
            {/if}
            Run Evaluation
          </button>
        </div>
      </div>

      <!-- Right Column: Results -->
      <div class="p-6 bg-gray-50 rounded-lg shadow-sm flex flex-col">
        <h2 class="text-2xl font-semibold text-gray-700 mb-4">Evaluation Results</h2>
        <div class="flex-grow bg-white p-4 rounded-lg border border-gray-200 overflow-auto max-h-[600px]">
          {#if evaluationResult}
            <div class="prose max-w-none">
              {@html evaluationResult}
            </div>
          {:else}
            <p class="text-gray-500">Results will be displayed here.</p>
          {/if}
        </div>
      </div>
    </div>
  </div>
</main>