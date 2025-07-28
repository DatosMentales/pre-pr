// Learn more about Tauri commands at https://tauri.app/develop/calling-rust/
#![allow(unused_variables)] // Allow unused variables for now

use serde::{Deserialize, Serialize};
use std::fs;
use std::env;

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
) -> Result<String, String> {
    // Read the content of the files
    let mut file_contents = Vec::new();
    for path in &file_paths {
        let content = fs::read_to_string(path).map_err(|e| e.to_string())?;
        file_contents.push(content);
    }

    // Read the content of the standards file
    let standards_content = fs::read_to_string(standards_path).map_err(|e| e.to_string())?;

    // Construct the prompt for the LLM
    let prompt = format!(
        "Eres un asistente de revisión de código. Analiza los siguientes archivos de código basándote en los estándares proporcionados. \n\n\n        Responde en formato Markdown. Para cada hallazgo, utiliza una notación similar a un diff: \n\n\n        - Usa '-' para incumplimientos o problemas. \n\n\n        - Usa '+' para recomendaciones o mejoras. \n\n\n        - Usa '!' para comentarios u observaciones generales. \n\n\n        Ejemplo: \n\n\n        ```diff\n\n\n        - src/main.rs: Línea 10: La función 'foo' carece de documentación.\n\n\n        + src/main.rs: Considera agregar pruebas unitarias para 'bar'.\n\n\n        ! src/utils.rs: La calidad general del código es buena.\n\n\n        ```\n\n\n        Estándares:\n{}\n\n\n        Archivos de Código:\n{}",
        standards_content,
        file_contents.join("\n---\n")
    );

    // Log the prompt length
    println!("Prompt length: {}", prompt.len());

    // Call the appropriate LLM API
    let response = match llm_provider.as_str() {
        "openai" => call_openai_api(&prompt, &api_key, &model).await?,
        "anthropic" => call_anthropic_api(&prompt, &api_key, &model).await?,
        "google" => call_google_api(&prompt, &api_key, &model).await?,
        "local" => call_local_llm(&prompt, &model).await?,
        _ => return Err("Invalid LLM provider".to_string()),
    };

    Ok(response)
}

async fn call_openai_api(prompt: &str, api_key: &str, model: &str) -> Result<String, String> {
    let client = reqwest::Client::new();
    let request_body = serde_json::json!({
        "model": model,
        "messages": [{"role": "user", "content": prompt}]
    });

    let response = client.post("https://api.openai.com/v1/chat/completions")
        .header("Authorization", format!("Bearer {}", api_key))
        .header("Content-Type", "application/json")
        .json(&request_body)
        .send()
        .await
        .map_err(|e| e.to_string())?;

    let response_json: serde_json::Value = response.json().await.map_err(|e| e.to_string())?;
    let content = response_json["choices"][0]["message"]["content"].as_str().unwrap_or("").to_string();
    Ok(content)
}

async fn call_anthropic_api(prompt: &str, api_key: &str, model: &str) -> Result<String, String> {
    let client = reqwest::Client::new();
    let request_body = serde_json::json!({
        "model": model,
        "prompt": format!("\n\nHuman: {}\n\nAssistant:", prompt),
        "max_tokens_to_sample": 1000,
    });

    let response = client.post("https://api.anthropic.com/v1/messages")
        .header("x-api-key", api_key)
        .header("anthropic-version", "2023-06-01")
        .header("Content-Type", "application/json")
        .json(&request_body)
        .send()
        .await
        .map_err(|e| e.to_string())?;

    let response_json: serde_json::Value = response.json().await.map_err(|e| e.to_string())?;
    let content = response_json["content"][0]["text"].as_str().unwrap_or("").to_string();
    Ok(content)
}

async fn call_google_api(prompt: &str, api_key: &str, model: &str) -> Result<String, String> {
    let client = reqwest::Client::new();
    let url = format!("https://generativelanguage.googleapis.com/v1beta/models/{}:generateContent?key={}", model, api_key);

    let request_body = serde_json::json!({
        "contents": [{
            "parts": [{
                "text": prompt
            }]
        }]
    });

    let response = client.post(&url)
        .header("Content-Type", "application/json")
        .json(&request_body)
        .send()
        .await
        .map_err(|e| e.to_string())?;

    let response_json: serde_json::Value = response.json().await.map_err(|e| e.to_string())?;
    println!("Google API full response: {:?}", response_json);

    let content = response_json["candidates"][0]["content"]["parts"][0]["text"].as_str().unwrap_or("").to_string();
    Ok(content)
}

async fn call_local_llm(prompt: &str, model: &str) -> Result<String, String> {
    let client = reqwest::Client::new();
    let (base_url, model_name) = if model.starts_with("Ollama:") {
        ("http://localhost:11434/api/generate", model.replace("Ollama: ", ""))
    } else if model.starts_with("LM Studio:") {
        ("http://localhost:1234/v1/chat/completions", model.replace("LM Studio: ", ""))
    } else {
        return Err("Invalid local LLM model format".to_string());
    };

    let request_body = if base_url.contains("ollama") {
        serde_json::json!({
            "model": model_name,
            "prompt": prompt,
            "stream": false,
            "keep_alive": "5m" // Add keep_alive for Ollama
        })
    } else {
        serde_json::json!({
            "model": model_name,
            "messages": [{"role": "user", "content": prompt}],
            "stream": false
        })
    };

    let response = client.post(base_url)
        .header("Content-Type", "application/json") // Explicitly set Content-Type
        .json(&request_body)
        .send()
        .await
        .map_err(|e| e.to_string())?;

    let response_json: serde_json::Value = response.json().await.map_err(|e| e.to_string())?;

    // Print the full JSON response for debugging
    println!("Full LLM response: {:?}", response_json);

    let content = if base_url.contains("ollama") {
        response_json["response"].as_str().unwrap_or("").to_string()
    } else {
        response_json["choices"][0]["message"]["content"].as_str().unwrap_or("").to_string()
    };

    if content.is_empty() {
        Ok(format!("LLM returned empty content. Full response: {}", response_json.to_string()))
    } else {
        Ok(content)
    }
}

#[tauri::command]
async fn save_llm_config(
    llm_provider: String,
    api_key: String,
    model: String,
) -> Result<(), String> {
    let config = LlmConfig { llm_provider, api_key, model };
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
        .invoke_handler(tauri::generate_handler![greet, get_local_llm_models, analyze_code, save_llm_config, load_llm_config])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}