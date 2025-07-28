// Learn more about Tauri commands at https://tauri.app/develop/calling-rust/
#![allow(unused_variables)] // Allow unused variables for now

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
    llm_provider: String,
    api_key: String,
    model: String,
) -> Result<String, String> {
    // Read the content of the files
    let mut file_contents = Vec::new();
    for path in &file_paths {
        let content = std::fs::read_to_string(path).map_err(|e| e.to_string())?;
        file_contents.push(content);
    }

    // Read the content of the standards file
    let standards_content = std::fs::read_to_string(standards_path).map_err(|e| e.to_string())?;

    // Construct the prompt for the LLM
    let prompt = format!(
        "Analyze the following code files based on the provided standards.\n\n\
        Standards:\n{}\n\n\
        Code Files:\n{}",
        standards_content,
        file_contents.join("\n---\n")
    );

    // Log the prompt length
    println!("Prompt length: {}", prompt.len());

    // Call the appropriate LLM API
    let response = match llm_provider.as_str() {
        "openai" => call_openai_api(&prompt, &api_key, &model).await?,
        "anthropic" => call_anthropic_api(&prompt, &api_key, &model).await?,
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

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .plugin(tauri_plugin_opener::init())
        .plugin(tauri_plugin_dialog::init())
        .plugin(tauri_plugin_fs::init())
        .invoke_handler(tauri::generate_handler![greet, get_local_llm_models, analyze_code])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}