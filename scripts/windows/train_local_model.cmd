@echo off
setlocal EnableExtensions

set "PROJECT_ROOT=%~dp0..\.."
pushd "%PROJECT_ROOT%" || exit /b 1

if "%CONDA_ENV_PATH%"=="" set "CONDA_ENV_PATH=E:\Mamba\envs\ml-gpu"
if "%LOCAL_MODEL_SOURCE%"=="" set "LOCAL_MODEL_SOURCE=/app/local_models/news-flow-ru-vectorization-mpnet/final"

echo [1/3] Training local model in %CONDA_ENV_PATH%
conda run -p "%CONDA_ENV_PATH%" python scripts\train_embeddings.py %*
if errorlevel 1 goto :error

if not exist "models\news-flow-ru-vectorization-mpnet\final\model.safetensors" (
  echo Local model was not found at models\news-flow-ru-vectorization-mpnet\final
  goto :error
)

echo [2/3] Building Docker images for local-model test mode
set "USE_LOCAL_MODEL=true"
set "PRELOAD_MODEL_FROM_HF=false"
docker compose build model-service
if errorlevel 1 goto :error

echo [3/3] Starting stack with local model mounted read-only
docker compose up -d
if errorlevel 1 goto :error

echo.
echo Local model test stack is running.
echo Model source inside container: %LOCAL_MODEL_SOURCE%
popd
exit /b 0

:error
echo.
echo train_local_model.cmd failed.
popd
exit /b 1
