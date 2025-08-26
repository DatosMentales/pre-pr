// Learn more about Tauri commands at https://tauri.app/develop/calling-rust/
#![allow(unused_variables)] // Allow unused variables for now

use serde::{Deserialize, Serialize};
use std::env;
use std::fs;

#[derive(Debug, Serialize, Deserialize)]
struct LlmConfig {
    llm_provider: String,
    api_key: String,
    model: String,
}

#[tauri::command]
fn greet(name: &str) -> String {
    format!("Hello, {}! You've been greeted from Rust!", name)
}

#[tauri::command]
async fn get_local_llm_models() -> Result<Vec<String>, String> {
    let mut models = Vec::new();

    // Try Ollama
    let ollama_url = "http://localhost:11434/api/tags";
    match reqwest::get(ollama_url).await {
        Ok(response) => {
            if response.status().is_success() {
                let json: serde_json::Value = response.json().await.map_err(|e| e.to_string())?;
                if let Some(ollama_models) = json["models"].as_array() {
                    for model in ollama_models {
                        if let Some(name) = model["name"].as_str() {
                            models.push(format!("Ollama: {}", name));
                        }
                    }
                }
            }
        }
        Err(_) => { /* Ollama not running or unreachable */ }
    }

    // Try LM Studio
    let lmstudio_url = "http://localhost:1234/v1/models";
    match reqwest::get(lmstudio_url).await {
        Ok(response) => {
            if response.status().is_success() {
                let json: serde_json::Value = response.json().await.map_err(|e| e.to_string())?;
                if let Some(lm_models) = json["data"].as_array() {
                    for model in lm_models {
                        if let Some(id) = model["id"].as_str() {
                            models.push(format!("LM Studio: {}", id));
                        }
                    }
                }
            }
        }
        Err(_) => { /* LM Studio not running or unreachable */ }
    }

    Ok(models)
}

#[tauri::command]
async fn analyze_code(
    file_paths: Vec<String>,
    standards_path: String,
    example_file_path: Option<String>,
    llm_provider: String,
    api_key: String,
    model: String,
    output_language: String,
    additional_context: Option<String>,
) -> Result<String, String> {
    // Read the content of the files to be analyzed
    let mut file_contents = Vec::new();
    for path in &file_paths {
        let content =
            fs::read_to_string(path).map_err(|e| format!("Failed to read file {}: {}", path, e))?;
        file_contents.push(content);
    }

    // Read the content of the standards file
    let standards_content = fs::read_to_string(&standards_path)
        .map_err(|e| format!("Failed to read standards file {}: {}", standards_path, e))?;

    // Read the content of the example file, if provided
    let example_content = match example_file_path {
        Some(path) => fs::read_to_string(&path)
            .map_err(|e| format!("Failed to read example file {}: {}", path, e))?,
        None => String::new(),
    };

    // Construct the example section of the prompt only if there is content
    let example_prompt_section = if !example_content.is_empty() {
        format!(
            "\n\nAdicionalmente, aquí tienes un ejemplo de código que cumple con los estándares. Úsalo como referencia para tu análisis:\n\n--- INICIO EJEMPLO DE CÓDIGO ---\n{}\n--- FIN EJEMPLO DE CÓDIGO ---\n",
            example_content
        )
    } else {
        String::new()
    };

    // Construct the additional context section (e.g., table descriptions) if provided
    let additional_context_section = match &additional_context {
        Some(ctx) if !ctx.is_empty() => format!(
            "\n\n--- INICIO CONTEXTO ADICIONAL ---\n{}\n--- FIN CONTEXTO ADICIONAL ---\n",
            ctx
        ),
        _ => String::new(),
    };

    // Construct the final prompt for the LLM
    let prompt = format!(
        "Eres un asistente experto en revisión de código. Tu tarea es analizar los archivos de código proporcionados y compararlos con los estándares de codificación dados.

Responde en formato Markdown y en {}. Para cada hallazgo, utiliza una notación similar a un 'diff':
- Usa '-' para incumplimientos o problemas encontrados.
- Usa '+' para recomendaciones o posibles mejoras.
- Usa '!' para comentarios u observaciones generales.

Ejemplo de formato de respuesta:
```diff
- src/main.rs: Línea 10: La función 'foo' carece de documentación.
+ src/main.rs: Considera agregar pruebas unitarias para la función 'bar'.
! src/utils.rs: La calidad general del código es buena y sigue las convenciones.
```

--- INICIO ESTÁNDARES DE CODIFICACIÓN ---
{}
--- FIN ESTÁNDARES DE CODIFICACIÓN ---
--- INICIO DE ARCHIVO OBJETIVO ---
{}
--- FIN DE ARCHIVO OBJETIVO ---
{}--- INICIO ARCHIVOS DE CÓDIGO A ANALIZAR ---
{}
--- FIN ARCHIVOS DE CÓDIGO A ANALIZAR ---",
        output_language,
        standards_content,
        example_prompt_section, // This will be empty if no example was provided
        additional_context_section, // This will be empty if no additional context was provided
        file_contents.join("\n---\n")
    );

    // Log the prompt length for debugging
    println!("Prompt length: {} characters", prompt.len());

    // Call the appropriate LLM API based on the provider
    let response = match llm_provider.as_str() {
        "openai" => call_openai_api(&prompt, &api_key, &model).await?,
        "anthropic" => call_anthropic_api(&prompt, &api_key, &model).await?,
        "google" => call_google_api(&prompt, &api_key, &model).await?,
        "local" => call_local_llm(&prompt, &model).await?,
        _ => return Err("Proveedor de LLM no válido.".to_string()),
    };

    Ok(response)
}

async fn call_openai_api(prompt: &str, api_key: &str, model: &str) -> Result<String, String> {
    let client = reqwest::Client::new();
    let request_body = serde_json::json!({
        "model": model,
        "messages": [{"role": "user", "content": prompt}]
    });

    let response = client
        .post("https://api.openai.com/v1/chat/completions")
        .header("Authorization", format!("Bearer {}", api_key))
        .header("Content-Type", "application/json")
        .json(&request_body)
        .send()
        .await
        .map_err(|e| e.to_string())?;

    let response_json: serde_json::Value = response.json().await.map_err(|e| e.to_string())?;
    normalize_response(&response_json, "openai")
}

async fn call_anthropic_api(prompt: &str, api_key: &str, model: &str) -> Result<String, String> {
    let client = reqwest::Client::new();
    let request_body = serde_json::json!({
        "model": model,
        "max_tokens": 2048,
        "messages": [{"role": "user", "content": prompt}]
    });

    let response = client
        .post("https://api.anthropic.com/v1/messages")
        .header("x-api-key", api_key)
        .header("anthropic-version", "2023-06-01")
        .header("Content-Type", "application/json")
        .json(&request_body)
        .send()
        .await
        .map_err(|e| e.to_string())?;

    let response_json: serde_json::Value = response.json().await.map_err(|e| e.to_string())?;
    normalize_response(&response_json, "anthropic")
}

async fn call_google_api(prompt: &str, api_key: &str, model: &str) -> Result<String, String> {
    let client = reqwest::Client::new();
    let url = format!(
        "https://generativelanguage.googleapis.com/v1beta/models/{}:generateContent?key={}",
        model, api_key
    );

    let request_body = serde_json::json!({
        "contents": [{
            "parts": [{
                "text": prompt
            }]
        }]
    });

    let response = client
        .post(&url)
        .header("Content-Type", "application/json")
        .json(&request_body)
        .send()
        .await
        .map_err(|e| e.to_string())?;

    let response_json: serde_json::Value = response.json().await.map_err(|e| e.to_string())?;
    println!("Google API full response: {:?}", response_json);

    normalize_response(&response_json, "google")
}

async fn call_local_llm(prompt: &str, model: &str) -> Result<String, String> {
    let client = reqwest::Client::new();
    let (base_url, model_name, provider) = if model.starts_with("Ollama:") {
        (
            "http://localhost:11434/api/generate",
            model.replace("Ollama: ", ""),
            "ollama",
        )
    } else if model.starts_with("LM Studio:") {
        (
            "http://localhost:1234/v1/chat/completions",
            model.replace("LM Studio: ", ""),
            "lm_studio",
        )
    } else {
        return Err("Invalid local LLM model format".to_string());
    };

    let request_body = if provider == "ollama" {
        serde_json::json!({
            "model": model_name,
            "prompt": prompt,
            "stream": false,
            "keep_alive": "5m"
        })
    } else {
        serde_json::json!({
            "model": model_name,
            "messages": [{"role": "user", "content": prompt}],
            "stream": false
        })
    };

    let response = client
        .post(base_url)
        .header("Content-Type", "application/json")
        .json(&request_body)
        .send()
        .await
        .map_err(|e| e.to_string())?;

    let response_json: serde_json::Value = response.json().await.map_err(|e| e.to_string())?;
    println!("Full LLM response: {:?}", response_json);

    normalize_response(&response_json, provider)
}

fn normalize_response(response_json: &serde_json::Value, provider: &str) -> Result<String, String> {
    let content_result = match provider {
        "openai" | "lm_studio" => response_json
            .get("choices")
            .and_then(|c| c.get(0))
            .and_then(|m| m.get("message"))
            .and_then(|c| c.get("content"))
            .and_then(|s| s.as_str())
            .map(String::from),
        "anthropic" => response_json
            .get("content")
            .and_then(|c| c.get(0))
            .and_then(|t| t.get("text"))
            .and_then(|s| s.as_str())
            .map(String::from),
        "google" => response_json
            .get("candidates")
            .and_then(|c| c.get(0))
            .and_then(|c| c.get("content"))
            .and_then(|p| p.get("parts"))
            .and_then(|parts| parts.get(0)) // Get the first part directly
            .and_then(|part| part.get("text"))
            .and_then(|s| s.as_str())
            .map(String::from),
        "ollama" => response_json
            .get("response")
            .and_then(|s| s.as_str())
            .map(String::from),
        _ => return Err("Invalid LLM provider for normalization".to_string()),
    };

    match content_result {
        Some(content) if !content.is_empty() => Ok(content),
        _ => {
            let error_message = format!(
                "LLM returned empty or invalid content. Full response: {}",
                response_json.to_string()
            );
            println!("{}", error_message); // Log the error for debugging
            Err(error_message)
        }
    }
}

#[tauri::command]
async fn extract_tables(
    file_path: String,
    llm_provider: String,
    api_key: String,
    model: String,
) -> Result<String, String> {
    // Read the content of the file to analyze for table usage
    let file_content = fs::read_to_string(&file_path)
        .map_err(|e| format!("Failed to read file {}: {}", file_path, e))?;

    // Build a prompt that asks the model to extract physical table names and RETURN ONLY DESCRIBE statements.
    // Important: the model must output ONLY lines with DESCRIBE statements (one or more), each ending with a semicolon.
    // No explanations, no markdown fences, no additional text.
    let prompt = format!(
        "Eres un asistente experto en bases de datos y análisis de código. \
Analiza el siguiente código fuente y detecta todas las tablas físicas referenciadas en él. \
Por cada tabla detectada, devuelve ÚNICAMENTE una sentencia SQL 'DESCRIBE <tabla>;' terminada en punto y coma. \
No devuelvas ningún otro texto, comentarios, explicaciones ni bloques de código, solo las sentencias DESCRIBE separadas por saltos de línea.\n\n--- INICIO DEL ARCHIVO ---\n{}\n--- FIN DEL ARCHIVO ---\n\nRecuerda: Devuelve solamente las sentencias DESCRIBE; nada más.",
        file_content
    );

    // Call the appropriate LLM API based on the provider
    let response = match llm_provider.as_str() {
        "openai" => call_openai_api(&prompt, &api_key, &model).await?,
        "anthropic" => call_anthropic_api(&prompt, &api_key, &model).await?,
        "google" => call_google_api(&prompt, &api_key, &model).await?,
        "local" => call_local_llm(&prompt, &model).await?,
        _ => return Err("Proveedor de LLM no válido.".to_string()),
    };

    // Post-process the response to ensure we only return DESCRIBE statements.
    // Remove code fences and backticks, then split by semicolon to collect statements that include 'describe' (case-insensitive).
    let mut cleaned = response.replace("```", "");
    cleaned = cleaned.replace("`", "");
    // Normalize different line endings
    let cleaned = cleaned.replace("\r\n", "\n");
    let segments: Vec<&str> = cleaned.split(';').collect();
    let mut describes: Vec<String> = Vec::new();

    for seg in segments {
        let seg_trim = seg.trim();
        if seg_trim.is_empty() {
            continue;
        }
        // Check if the segment contains the word 'describe' as a standalone token (case-insensitive)
        if seg_trim.to_lowercase().contains("describe ")
            || seg_trim.to_lowercase().starts_with("describe")
            || seg_trim.to_lowercase().starts_with("desc ")
        {
            // reconstruct with semicolon
            describes.push(format!("{};", seg_trim.trim()));
        }
    }

    if !describes.is_empty() {
        // join with newline and return
        Ok(describes.join("\n"))
    } else {
        // No explicit DESCRIBE lines found — return an error indicating the model didn't follow the constraints,
        // and include the raw response to help debugging.
        let error_message = format!(
            "LLM did not return any DESCRIBE statements. Full response:\n{}",
            response
        );
        println!("{}", error_message);
        Err(error_message)
    }
}

#[tauri::command]
async fn save_llm_config(
    llm_provider: String,
    api_key: String,
    model: String,
) -> Result<(), String> {
    let config = LlmConfig {
        llm_provider,
        api_key,
        model,
    };
    let config_json = serde_json::to_string_pretty(&config).map_err(|e| e.to_string())?;

    let current_dir = env::current_dir().map_err(|e| e.to_string())?;
    let config_path = current_dir.join("model.json");

    fs::write(&config_path, config_json).map_err(|e| e.to_string())?;

    Ok(())
}

#[tauri::command]
async fn load_llm_config() -> Result<LlmConfig, String> {
    let current_dir = env::current_dir().map_err(|e| e.to_string())?;
    let config_path = current_dir.join("model.json");

    if !config_path.exists() {
        return Err("No saved configuration found.".to_string());
    }

    let config_json = fs::read_to_string(&config_path).map_err(|e| e.to_string())?;
    let config: LlmConfig = serde_json::from_str(&config_json).map_err(|e| e.to_string())?;

    Ok(config)
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .plugin(tauri_plugin_opener::init())
        .plugin(tauri_plugin_dialog::init())
        .plugin(tauri_plugin_fs::init())
        .plugin(tauri_plugin_shell::init())
        .invoke_handler(tauri::generate_handler![
            greet,
            get_local_llm_models,
            analyze_code,
            extract_tables,
            save_llm_config,
            load_llm_config
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
