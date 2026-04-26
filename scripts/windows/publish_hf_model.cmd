@echo off
setlocal EnableExtensions

set "PROJECT_ROOT=%~dp0..\.."
pushd "%PROJECT_ROOT%" || exit /b 1

if "%CONDA_ENV_PATH%"=="" set "CONDA_ENV_PATH=E:\Mamba\envs\ml-gpu"
if "%HF_MODEL_METADATA_PATH%"=="" set "HF_MODEL_METADATA_PATH=/app/configs/model_registry/latest_model.json"
if "%HF_MODEL_DIR%"=="" set "HF_MODEL_DIR=/app/models/news-flow-ru-vectorization-mpnet/final"
if "%REMOTE_MODEL_SOURCE%"=="" set "REMOTE_MODEL_SOURCE=/app/configs/model_registry/latest_model.json"

if "%HF_TOKEN%"=="" if exist "token.local" (
  set /p HF_TOKEN=<token.local
)

echo [1/3] Publishing current local model to Hugging Face Hub
conda run -p "%CONDA_ENV_PATH%" python scripts\publish_model.py %*
if errorlevel 1 goto :error

echo [2/4] Building model-service image with model preloaded from Hugging Face
set "USE_LOCAL_MODEL=false"
set "PRELOAD_MODEL_FROM_HF=true"
set "DOCKER_BUILDKIT=1"

if "%HF_TOKEN%"=="" (
  docker build ^
    --build-arg PRELOAD_MODEL_FROM_HF=true ^
    --build-arg HF_MODEL_METADATA_PATH="%HF_MODEL_METADATA_PATH%" ^
    --build-arg HF_MODEL_DIR="%HF_MODEL_DIR%" ^
    -f docker\model-service.Dockerfile ^
    -t news-flow-model-service:local ^
    .
) else (
  docker build ^
    --secret id=hf_token,env=HF_TOKEN ^
    --build-arg PRELOAD_MODEL_FROM_HF=true ^
    --build-arg HF_MODEL_METADATA_PATH="%HF_MODEL_METADATA_PATH%" ^
    --build-arg HF_MODEL_DIR="%HF_MODEL_DIR%" ^
    -f docker\model-service.Dockerfile ^
    -t news-flow-model-service:local ^
    .
)
if errorlevel 1 goto :error

echo [3/4] Building API image
docker compose build api
if errorlevel 1 goto :error

echo [4/4] Starting stack with HF-backed model image
docker compose up -d --no-build
if errorlevel 1 goto :error

echo.
echo Hugging Face model image is built and the stack is running.
echo Remote model source inside container: %REMOTE_MODEL_SOURCE%
echo Registry metadata: configs\model_registry\latest_model.json
popd
exit /b 0

:error
echo.
echo publish_hf_model.cmd failed.
popd
exit /b 1
