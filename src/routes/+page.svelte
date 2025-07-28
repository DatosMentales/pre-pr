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

<main class="container mx-auto p-8">
  <h1 class="text-3xl font-bold mb-6">PR Standards Evaluator</h1>

  <div class="grid grid-cols-1 md:grid-cols-2 gap-8">
    <!-- Left Column: Configuration -->
    <div>
      <div class="mb-6">
        <h2 class="text-xl font-bold mb-2">1. Select Source Files</h2>
        <button on:click={selectFiles} class="btn btn-primary">
          {selectedFiles.length > 0 ? `${selectedFiles.length} files selected` : 'Select Files'}
        </button>
        <ul class="mt-2">
          {#each selectedFiles as file}
            <li class="text-sm">{file}</li>
          {/each}
        </ul>
      </div>

      <div class="mb-6">
        <h2 class="text-xl font-bold mb-2">2. Select Standards File</h2>
        <button on:click={selectStandardsFile} class="btn btn-primary">
          {standardsFile ? standardsFile : 'Select Standards (.txt, .json)'}
        </button>
      </div>

      <div class="mb-6">
        <h2 class="text-xl font-bold mb-2">3. Configure LLM</h2>
        <div class="form-control w-full max-w-xs">
          <label class="label" for="llm-provider">
            <span class="label-text">LLM Provider</span>
          </label>
          <select id="llm-provider" bind:value={llmProvider} class="select select-bordered">
            <option value="openai">OpenAI</option>
            <option value="anthropic">Anthropic</option>
            <option value="local">Local (Ollama/LMStudio)</option>
          </select>
        </div>

        {#if llmProvider !== 'local'}
          <div class="form-control w-full max-w-xs mt-4">
            <label class="label" for="api-key">
              <span class="label-text">API Key</span>
            </label>
            <input id="api-key" type="password" bind:value={apiKey} placeholder="Enter your API key" class="input input-bordered w-full max-w-xs" />
          </div>
          <div class="form-control w-full max-w-xs mt-4">
            <label class="label" for="model">
              <span class="label-text">Model Name</span>
            </label>
            <input id="model" type="text" bind:value={model} placeholder="e.g., gpt-4, claude-2" class="input input-bordered w-full max-w-xs" />
          </div>
        {:else}
          <div class="form-control w-full max-w-xs mt-4">
            <label class="label" for="local-model">
              <span class="label-text">Local Model</span>
            </label>
            <select id="local-model" bind:value={model} class="select select-bordered" disabled={isLoading}>
              {#each localModels as localModel}
                <option value={localModel}>{localModel}</option>
              {:else}
                <option value="" disabled>No local models found</option>
              {/each}
            </select>
          </div>
        {/if}

        <button on:click={runEvaluation} class="btn btn-secondary w-full" disabled={isLoading}>
          {#if isLoading}
            <span class="loading loading-spinner"></span>
          {/if}
          Run Evaluation
        </button>
      </div>

      <!-- Right Column: Results -->
      <div class="bg-white p-6 rounded-lg shadow-md">
        <h2 class="text-2xl font-bold mb-4">Evaluation Results</h2>
        {#if evaluationResult}
          <div class="prose">
            {@html evaluationResult}
          </div>
        {:else}
          <p>Results will be displayed here.</p>
        {/if}
      </div>
    </div>
  </main>
