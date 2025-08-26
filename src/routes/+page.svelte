<script lang="ts">
    import { open } from "@tauri-apps/plugin-dialog";
    import { invoke } from "@tauri-apps/api/core";

    let selectedFiles = $state<string[]>([]);
    let standardsFile = $state<string | null>(null);
    let exampleFile = $state<string | null>(null);
    let llmProvider = $state<"openai" | "anthropic" | "google" | "local">(
        "openai",
    );
    let apiKey = $state("");
    let model = $state("");
    let outputLanguage = $state("english");
    let evaluationResult = $state("");
    let evaluationError = $state<string | null>(null);
    let isLoading = $state(false);
    let localModels = $state<string[]>([]);
    let darkMode = $state(false); // State for dark mode

    // New states for table extraction
    let extractedContext = $state(""); // The SQL DESCRIBE/CREATE results returned by the extraction
    let extractedError = $state<string | null>(null);
    let isExtracting = $state(false);
    let includeExtracted = $state(false); // When true, the extracted context will be passed to analysis as additional_context

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
            filters: [
                {
                    name: "Standards",
                    extensions: ["txt", "json", "md"],
                },
            ],
        });
        if (typeof result === "string") {
            standardsFile = result;
        }
    }

    async function selectExampleFile() {
        const result = await open({
            multiple: false,
        });
        if (typeof result === "string") {
            exampleFile = result;
        }
    }

    async function fetchLocalModels() {
        try {
            isLoading = true;
            const result = await invoke("get_local_llm_models");
            localModels = result as string[];
            if (localModels.length > 0) {
                model = localModels[0]; // Select the first model by default
            } else {
                model = "";
            }
        } catch (error) {
            console.error("Error fetching local models:", error);
            localModels = [];
            model = "";
            alert(`Failed to fetch local models: ${error}`);
        } finally {
            isLoading = false;
        }
    }

    // Fetch local models when the provider changes to 'local'
    $effect(() => {
        if (llmProvider === "local") {
            fetchLocalModels();
        }
    });

    // Toggle dark mode
    function toggleDarkMode() {
        darkMode = !darkMode;
    }

    $effect(() => {
        document.documentElement.classList.toggle("dark", darkMode);
    });

    async function saveConfig() {
        try {
            await invoke("save_llm_config", {
                llmProvider: llmProvider,
                apiKey: apiKey,
                model: model,
            });
            alert("Configuration saved successfully!");
        } catch (error) {
            alert(`Failed to save configuration: ${error}`);
        }
    }

    async function loadConfig() {
        try {
            // `invoke` returns `unknown` from JS; cast to `any` so we can access properties safely.
            const config: any = await invoke("load_llm_config");
            // Ensure properties exist before assigning to avoid runtime errors
            if (config && typeof config === "object") {
                llmProvider = (config.llm_provider ??
                    config.llmProvider) as typeof llmProvider;
                apiKey = (config.api_key ?? config.apiKey) as string;
                model = (config.model ?? config.model) as string;
                alert("Configuration loaded successfully!");
            } else {
                alert("Loaded configuration was empty or invalid.");
            }
        } catch (error) {
            alert(`Failed to load configuration: ${error}`);
        }
    }

    function clearAll() {
        selectedFiles = [];
        standardsFile = null;
        exampleFile = null;
        evaluationResult = "";
        evaluationError = null;

        // clear extraction-related state too
        extractedContext = "";
        extractedError = null;
        includeExtracted = false;
    }

    async function copyToClipboard() {
        if (evaluationResult) {
            try {
                await navigator.clipboard.writeText(evaluationResult);
                alert("Results copied to clipboard!");
            } catch (err) {
                alert("Failed to copy results.");
            }
        }
    }

    async function runEvaluation() {
        if (selectedFiles.length === 0 || !standardsFile) {
            alert("Please select source files and a standards file.");
            return;
        }

        if (llmProvider !== "local" && !apiKey) {
            alert("Please enter an API Key.");
            return;
        }

        if (!model) {
            alert("Please select or enter a model name.");
            return;
        }

        isLoading = true;
        evaluationResult = "";
        evaluationError = null;

        try {
            // Pass the extracted context when includeExtracted is true
            const additionalContext =
                includeExtracted && extractedContext ? extractedContext : null;

            const result = await invoke("analyze_code", {
                filePaths: selectedFiles,
                standardsPath: standardsFile,
                exampleFilePath: exampleFile,
                llmProvider: llmProvider,
                apiKey: apiKey,
                model: model,
                outputLanguage: outputLanguage,
                additionalContext: additionalContext,
            });
            if (
                typeof result === "object" &&
                result !== null &&
                "Ok" in result
            ) {
                evaluationResult = result.Ok as string;
            } else if (
                typeof result === "object" &&
                result !== null &&
                "Err" in result
            ) {
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

    // New function: extractTables
    async function extractTables() {
        if (selectedFiles.length === 0) {
            alert(
                "Please select at least one source file to extract tables from.",
            );
            return;
        }

        // We'll operate on the first selected file (the UI communicates that)
        const targetFile = selectedFiles[0];

        if (llmProvider !== "local" && !apiKey) {
            alert(
                "Please enter an API Key to use remote LLM providers for extraction.",
            );
            return;
        }

        if (!model) {
            alert("Please select or enter a model name to use for extraction.");
            return;
        }

        // Warn if multiple files were selected (we only inspect the first)
        if (selectedFiles.length > 1) {
            const proceed = confirm(
                "Multiple files selected. Extraction will run against the first file only. Continue?",
            );
            if (!proceed) return;
        }

        isExtracting = true;
        extractedContext = "";
        extractedError = null;

        try {
            const result = await invoke("extract_tables", {
                filePath: targetFile,
                llmProvider: llmProvider,
                apiKey: apiKey,
                model: model,
            });

            if (
                typeof result === "object" &&
                result !== null &&
                "Ok" in result
            ) {
                extractedContext = result.Ok as string;
                // Optionally auto-include extracted context
                // includeExtracted = true;
            } else if (
                typeof result === "object" &&
                result !== null &&
                "Err" in result
            ) {
                extractedError = result.Err as string;
            } else {
                extractedContext = result as string;
            }
        } catch (error) {
            extractedError = `Extraction error: ${error}`;
        } finally {
            isExtracting = false;
        }
    }

    function loadExtractedIntoContext() {
        if (!extractedContext) {
            alert("No extracted context available to include.");
            return;
        }
        includeExtracted = true;
        alert("Extracted context will be included in the next evaluation.");
    }

    // Handle uploaded file (user supplies the output of DESCRIBE statements)
    async function handleDescribeUpload(event: Event) {
        const input = event.target as HTMLInputElement | null;
        const files = input?.files;
        if (!files || files.length === 0) {
            alert("No file selected.");
            return;
        }
        try {
            const file = files[0];
            // Use the browser File API to read file contents
            const text = await file.text();
            // Set the extracted context to uploaded content (user-provided DESCRIBE results)
            extractedContext = text;
            extractedError = null;
            alert(
                "Upload successful. DESCRIBE results loaded as extracted context.",
            );
        } catch (err) {
            extractedError = `Failed to read uploaded file: ${err}`;
        }
    }

    async function copyExtractedToClipboard() {
        if (!extractedContext) {
            alert("No extracted context to copy.");
            return;
        }
        try {
            await navigator.clipboard.writeText(extractedContext);
            alert("Extracted context copied to clipboard!");
        } catch (err) {
            alert("Failed to copy extracted context.");
        }
    }

    function clearExtracted() {
        extractedContext = "";
        extractedError = null;
        includeExtracted = false;
    }
</script>

<main
    class="min-h-screen bg-gray-100 dark:bg-gray-900 p-8 flex items-center justify-center transition-colors duration-300"
>
    <div
        class="bg-white dark:bg-gray-800 rounded-lg shadow-xl p-8 w-full max-w-7xl lg:max-w-full xl:max-w-screen-xl 2xl:max-w-screen-2xl transition-colors duration-300"
    >
        <div class="flex justify-between items-center mb-8">
            <h1
                class="text-4xl font-extrabold text-gray-800 dark:text-white text-center flex-grow"
            >
                PR Standards Evaluator
            </h1>
            <button
                onclick={toggleDarkMode}
                class="p-2 rounded-full bg-gray-200 dark:bg-gray-700 text-gray-800 dark:text-white focus:outline-none focus:ring-2 focus:ring-blue-500 transition-colors duration-300"
            >
                {#if darkMode}
                    <!-- Sun icon for light mode -->
                    <svg
                        xmlns="http://www.w3.org/2000/svg"
                        class="h-6 w-6"
                        fill="none"
                        viewBox="0 0 24 24"
                        stroke="currentColor"
                    >
                        <path
                            stroke-linecap="round"
                            stroke-linejoin="round"
                            stroke-width="2"
                            d="M12 3v1m0 16v1m9-9h1M3 12H2m15.325-4.775l-.707-.707M6.343 6.343l-.707-.707m12.728 12.728l-.707-.707M6.343 17.657l-.707-.707M16 12a4 4 0 11-8 0 4 4 0 018 0z"
                        />
                    </svg>
                {:else}
                    <!-- Moon icon for dark mode -->
                    <svg
                        xmlns="http://www.w3.org/2000/svg"
                        class="h-6 w-6"
                        fill="none"
                        viewBox="0 0 24 24"
                        stroke="currentColor"
                    >
                        <path
                            stroke-linecap="round"
                            stroke-linejoin="round"
                            stroke-width="2"
                            d="M20.354 15.354A9 9 0 018.646 3.646 9.003 9.003 0 0012 21a9.003 9.003 0 008.354-5.646z"
                        />
                    </svg>
                {/if}
            </button>
        </div>

        <div class="grid grid-cols-1 md:grid-cols-2 gap-12">
            <!-- Left Column: Configuration & Extraction -->
            <div>
                <div
                    class="mb-8 p-6 bg-gray-50 dark:bg-gray-700 rounded-lg shadow-sm transition-colors duration-300"
                >
                    <h2
                        class="text-2xl font-semibold text-gray-700 dark:text-white mb-4"
                    >
                        1. Select Source Files
                    </h2>
                    <button
                        onclick={selectFiles}
                        class="w-full bg-blue-600 hover:bg-blue-700 text-white font-bold py-3 px-6 rounded-lg transition duration-300 ease-in-out transform hover:scale-105 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:ring-opacity-50"
                    >
                        {selectedFiles.length > 0
                            ? `${selectedFiles.length} files selected`
                            : "Select Files"}
                    </button>
                    <ul
                        class="mt-4 space-y-2 text-gray-600 dark:text-gray-300 text-sm"
                    >
                        {#each selectedFiles as file}
                            <li
                                class="bg-gray-100 dark:bg-gray-600 p-2 rounded-md truncate transition-colors duration-300"
                            >
                                {file}
                            </li>
                        {/each}
                    </ul>

                    <!-- Button to extract tables from first file -->
                    <div class="mt-4">
                        <button
                            onclick={extractTables}
                            class="w-full bg-indigo-600 hover:bg-indigo-700 text-white font-bold py-2 px-4 rounded-lg transition duration-200"
                            disabled={isExtracting || isLoading}
                        >
                            {#if isExtracting}
                                <span
                                    class="inline-block w-4 h-4 border-2 border-t-2 border-white rounded-full animate-spin mr-2"
                                ></span>
                                Extracting tables...
                            {:else}
                                Extract Tables (from first file)
                            {/if}
                        </button>
                        <p
                            class="text-xs text-gray-500 dark:text-gray-300 mt-2"
                        >
                            This will send the first selected file to the chosen
                            LLM provider to extract physical tables and generate
                            DESCRIBE/CREATE statements.
                        </p>
                    </div>
                </div>

                <div
                    class="mb-8 p-6 bg-gray-50 dark:bg-gray-700 rounded-lg shadow-sm transition-colors duration-300"
                >
                    <h2
                        class="text-2xl font-semibold text-gray-700 dark:text-white mb-4"
                    >
                        2. Select Standards File
                    </h2>
                    <button
                        onclick={selectStandardsFile}
                        class="w-full bg-blue-600 hover:bg-blue-700 text-white font-bold py-3 px-6 rounded-lg transition duration-300 ease-in-out transform hover:scale-105 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:ring-opacity-50"
                    >
                        {standardsFile
                            ? standardsFile
                            : "Select Standards (.txt, .json, .md)"}
                    </button>
                    {#if standardsFile}
                        <p
                            class="mt-4 text-gray-600 dark:text-gray-300 text-sm bg-gray-100 dark:bg-gray-600 p-2 rounded-md truncate transition-colors duration-300"
                        >
                            {standardsFile}
                        </p>
                    {/if}
                </div>

                <div
                    class="mb-8 p-6 bg-gray-50 dark:bg-gray-700 rounded-lg shadow-sm transition-colors duration-300"
                >
                    <h2
                        class="text-2xl font-semibold text-gray-700 dark:text-white mb-4"
                    >
                        3. Select Example File (Optional)
                    </h2>
                    <button
                        onclick={selectExampleFile}
                        class="w-full bg-blue-600 hover:bg-blue-700 text-white font-bold py-3 px-6 rounded-lg transition duration-300 ease-in-out transform hover:scale-105 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:ring-opacity-50"
                    >
                        {exampleFile ? exampleFile : "Select Example File"}
                    </button>
                    {#if exampleFile}
                        <p
                            class="mt-4 text-gray-600 dark:text-gray-300 text-sm bg-gray-100 dark:bg-gray-600 p-2 rounded-md truncate transition-colors duration-300"
                        >
                            {exampleFile}
                        </p>
                    {/if}
                </div>

                <div
                    class="mb-8 p-6 bg-gray-50 dark:bg-gray-700 rounded-lg shadow-sm transition-colors duration-300"
                >
                    <h2
                        class="text-2xl font-semibold text-gray-700 dark:text-white mb-4"
                    >
                        4. Configure LLM
                    </h2>
                    <div class="mb-4">
                        <label
                            class="block text-gray-700 dark:text-gray-200 text-sm font-bold mb-2"
                            for="llm-provider"
                        >
                            LLM Provider
                        </label>
                        <div class="relative">
                            <select
                                id="llm-provider"
                                bind:value={llmProvider}
                                class="block appearance-none w-full bg-white dark:bg-gray-600 border border-gray-300 dark:border-gray-500 text-gray-700 dark:text-white py-3 px-4 pr-8 rounded-lg leading-tight focus:outline-none focus:bg-white dark:focus:bg-gray-600 focus:border-blue-500 shadow-sm transition-colors duration-300"
                            >
                                <option value="openai">OpenAI</option>
                                <option value="anthropic">Anthropic</option>
                                <option value="google">Google (Gemini)</option>
                                <option value="local"
                                    >Local (Ollama/LMStudio)</option
                                >
                            </select>
                            <div
                                class="pointer-events-none absolute inset-y-0 right-0 flex items-center px-2 text-gray-700 dark:text-gray-200"
                            >
                                <svg
                                    class="fill-current h-4 w-4"
                                    xmlns="http://www.w3.org/2000/svg"
                                    viewBox="0 0 20 20"
                                    ><path
                                        d="M9.293 12.95l.707.707L15.657 8l-1.414-1.414L10 10.828 5.757 6.586 4.343 8z"
                                    /></svg
                                >
                            </div>
                        </div>
                    </div>

                    {#if llmProvider !== "local"}
                        <div class="mb-4">
                            <label
                                class="block text-gray-700 dark:text-gray-200 text-sm font-bold mb-2"
                                for="api-key"
                            >
                                API Key
                            </label>
                            <input
                                id="api-key"
                                type="password"
                                bind:value={apiKey}
                                placeholder="Enter your API key"
                                class="shadow-sm appearance-none border rounded-lg w-full py-3 px-4 text-gray-700 dark:text-white dark:bg-gray-600 dark:border-gray-500 leading-tight focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent transition-colors duration-300"
                            />
                        </div>
                        <div class="mb-4">
                            <label
                                class="block text-gray-700 dark:text-gray-200 text-sm font-bold mb-2"
                                for="model"
                            >
                                Model Name
                            </label>
                            <input
                                id="model"
                                type="text"
                                bind:value={model}
                                placeholder="e.g., gpt-4, claude-2"
                                class="shadow-sm appearance-none border rounded-lg w-full py-3 px-4 text-gray-700 dark:text-white dark:bg-gray-600 dark:border-gray-500 leading-tight focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent transition-colors duration-300"
                            />
                        </div>
                    {:else}
                        <div class="mb-4">
                            <label
                                class="block text-gray-700 dark:text-gray-200 text-sm font-bold mb-2"
                                for="local-model"
                            >
                                Local Model
                            </label>
                            <div class="relative">
                                <select
                                    id="local-model"
                                    bind:value={model}
                                    class="block appearance-none w-full bg-white dark:bg-gray-600 border border-gray-300 dark:border-gray-500 text-gray-700 dark:text-white py-3 px-4 pr-8 rounded-lg leading-tight focus:outline-none focus:bg-white dark:focus:bg-gray-600 focus:border-blue-500 shadow-sm transition-colors duration-300"
                                    disabled={isLoading}
                                >
                                    {#each localModels as localModel}
                                        <option value={localModel}
                                            >{localModel}</option
                                        >
                                    {:else}
                                        <option value="" disabled
                                            >No local models found</option
                                        >
                                    {/each}
                                </select>
                                <div
                                    class="pointer-events-none absolute inset-y-0 right-0 flex items-center px-2 text-gray-700 dark:text-gray-200"
                                >
                                    <svg
                                        class="fill-current h-4 w-4"
                                        xmlns="http://www.w3.org/2000/svg"
                                        viewBox="0 0 20 20"
                                        ><path
                                            d="M9.293 12.95l.707.707L15.657 8l-1.414-1.414L10 10.828 5.757 6.586 4.343 8z"
                                        /></svg
                                    >
                                </div>
                            </div>
                        </div>
                    {/if}

                    <div class="mb-4">
                        <label
                            class="block text-gray-700 dark:text-gray-200 text-sm font-bold mb-2"
                            for="output-language"
                        >
                            Output Language
                        </label>
                        <div class="relative">
                            <select
                                id="output-language"
                                bind:value={outputLanguage}
                                class="block appearance-none w-full bg-white dark:bg-gray-600 border border-gray-300 dark:border-gray-500 text-gray-700 dark:text-white py-3 px-4 pr-8 rounded-lg leading-tight focus:outline-none focus:bg-white dark:focus:bg-gray-600 focus:border-blue-500 shadow-sm transition-colors duration-300"
                            >
                                <option value="english">English</option>
                                <option value="spanish">Spanish</option>
                            </select>
                            <div
                                class="pointer-events-none absolute inset-y-0 right-0 flex items-center px-2 text-gray-700 dark:text-gray-200"
                            >
                                <svg
                                    class="fill-current h-4 w-4"
                                    xmlns="http://www.w3.org/2000/svg"
                                    viewBox="0 0 20 20"
                                    ><path
                                        d="M9.293 12.95l.707.707L15.657 8l-1.414-1.414L10 10.828 5.757 6.586 4.343 8z"
                                    /></svg
                                >
                            </div>
                        </div>
                    </div>

                    <div class="flex space-x-4 mt-6">
                        <button
                            onclick={saveConfig}
                            class="flex-1 bg-gray-600 hover:bg-gray-700 text-white font-bold py-3 px-6 rounded-lg transition duration-300 ease-in-out transform hover:scale-105 focus:outline-none focus:ring-2 focus:ring-gray-500 focus:ring-opacity-50"
                        >
                            Save Config
                        </button>
                        <button
                            onclick={loadConfig}
                            class="flex-1 bg-gray-600 hover:bg-gray-700 text-white font-bold py-3 px-6 rounded-lg transition duration-300 ease-in-out transform hover:scale-105 focus:outline-none focus:ring-2 focus:ring-gray-500 focus:ring-opacity-50"
                        >
                            Load Config
                        </button>
                    </div>

                    <button
                        onclick={clearAll}
                        class="w-full bg-red-600 hover:bg-red-700 text-white font-bold py-3 px-6 rounded-lg transition duration-300 ease-in-out transform hover:scale-105 focus:outline-none focus:ring-2 focus:ring-red-500 focus:ring-opacity-50 mt-4"
                    >
                        Clear
                    </button>

                    <button
                        onclick={runEvaluation}
                        class="w-full bg-green-600 hover:bg-green-700 text-white font-bold py-3 px-6 rounded-lg transition duration-300 ease-in-out transform hover:scale-105 focus:outline-none focus:ring-2 focus:ring-green-500 focus:ring-opacity-50 mt-4"
                        disabled={isLoading}
                    >
                        {#if isLoading}
                            <span
                                class="inline-block w-5 h-5 border-2 border-t-2 border-white rounded-full animate-spin mr-2"
                            ></span>
                        {/if}
                        Run Evaluation
                    </button>
                </div>
            </div>

            <!-- Right Column: Extracted Context & Results -->
            <div
                class="p-6 bg-gray-50 dark:bg-gray-700 rounded-lg shadow-sm flex flex-col transition-colors duration-300"
            >
                <div class="flex justify-between items-center mb-4">
                    <h2
                        class="text-2xl font-semibold text-gray-700 dark:text-white"
                    >
                        Extracted DB Context
                    </h2>
                    <div class="flex items-center space-x-2">
                        <label
                            class="text-sm text-gray-600 dark:text-gray-300 inline-flex items-center"
                        >
                            <input
                                id="include-extracted-checkbox"
                                type="checkbox"
                                bind:checked={includeExtracted}
                                class="form-checkbox h-5 w-5 text-indigo-600 mr-2"
                            />
                            <span>Include in evaluation</span>
                        </label>
                    </div>
                </div>

                <div class="mb-4">
                    {#if isExtracting}
                        <div
                            class="p-4 bg-white dark:bg-gray-900 rounded-md border border-gray-200 dark:border-gray-600"
                        >
                            <p class="text-gray-600 dark:text-gray-300">
                                Extracting tables... Please wait.
                            </p>
                        </div>
                    {:else if extractedError}
                        <div
                            class="p-4 bg-white dark:bg-gray-900 rounded-md border border-red-200 dark:border-red-600"
                        >
                            <p class="text-red-600 dark:text-red-400">
                                {extractedError}
                            </p>
                        </div>
                    {:else if extractedContext}
                        <div
                            class="p-4 bg-white dark:bg-gray-900 rounded-md border border-gray-200 dark:border-gray-600 overflow-auto max-h-48"
                        >
                            <pre
                                class="whitespace-pre-wrap text-sm text-gray-800 dark:text-white">{extractedContext}</pre>
                        </div>

                        <div class="mt-2 flex items-center space-x-3">
                            <label
                                class="text-sm text-gray-600 dark:text-gray-300 mr-2"
                                >Upload DESCRIBE results:</label
                            >
                            <input
                                type="file"
                                accept=".sql,.txt"
                                onchange={handleDescribeUpload}
                                class="text-sm rounded-md border p-1 bg-white dark:bg-gray-800 dark:text-white"
                            />
                        </div>

                        <div class="flex space-x-2 mt-3">
                            <button
                                onclick={copyExtractedToClipboard}
                                class="flex-1 bg-gray-600 hover:bg-gray-700 text-white font-bold py-2 px-4 rounded-lg transition duration-200"
                                >Copy</button
                            >
                            <button
                                onclick={loadExtractedIntoContext}
                                class="flex-1 bg-indigo-600 hover:bg-indigo-700 text-white font-bold py-2 px-4 rounded-lg transition duration-200"
                                >Load into evaluation</button
                            >
                            <button
                                onclick={clearExtracted}
                                class="flex-1 bg-red-600 hover:bg-red-700 text-white font-bold py-2 px-4 rounded-lg transition duration-200"
                                >Clear</button
                            >
                        </div>
                    {:else}
                        <div
                            class="p-4 bg-white dark:bg-gray-900 rounded-md border border-gray-200 dark:border-gray-600"
                        >
                            <p class="text-gray-500 dark:text-gray-400 text-sm">
                                No extracted context yet. Use "Extract Tables"
                                to analyze the first selected file and generate
                                DESCRIBE statements. You can also upload a file
                                containing DESCRIBE results to include them as
                                additional context.
                            </p>
                        </div>
                    {/if}
                </div>

                <div class="flex justify-between items-center mb-4">
                    <h2
                        class="text-2xl font-semibold text-gray-700 dark:text-white"
                    >
                        Evaluation Results
                    </h2>
                    <button
                        onclick={copyToClipboard}
                        class="bg-gray-600 hover:bg-gray-700 text-white font-bold py-2 px-4 rounded-lg transition duration-300 ease-in-out transform hover:scale-105 focus:outline-none focus:ring-2 focus:ring-gray-500 focus:ring-opacity-50"
                    >
                        Copy
                    </button>
                </div>

                <div
                    class="flex-grow bg-white dark:bg-gray-900 p-4 rounded-lg border border-gray-200 dark:border-gray-600 overflow-auto h-[calc(100vh-200px)] transition-colors duration-300 text-gray-800 dark:text-white"
                >
                    {#if evaluationError}
                        <p class="text-red-500">{evaluationError}</p>
                    {:else if evaluationResult}
                        <div class="prose dark:prose-invert max-w-none">
                            {#each evaluationResult.split("\n") as line}
                                {#if line.startsWith("-")}
                                    <p class="diff-removed">{line}</p>
                                {:else if line.startsWith("+")}
                                    <p class="diff-added">{line}</p>
                                {:else if line.startsWith("!")}
                                    <p class="diff-comment">{line}</p>
                                {:else}
                                    <p>{line}</p>
                                {/if}
                            {/each}
                        </div>
                    {:else}
                        <p class="text-gray-500 dark:text-gray-400">
                            Results will be displayed here.
                        </p>
                    {/if}
                </div>
            </div>
        </div>
    </div>
</main>

<style lang="postcss">
    .diff-added {
        color: #10b981; /* green-600 */
        display: block;
    }
    .diff-removed {
        color: #ef4444; /* red-600 */
        display: block;
    }
    .diff-comment {
        color: #f59e0b; /* amber-500 (orange) */
        display: block;
    }
    pre {
        font-family:
            ui-monospace, SFMono-Regular, Menlo, Monaco, "Roboto Mono",
            "Courier New", monospace;
        font-size: 0.85rem;
    }
</style>
