$ErrorActionPreference = 'Stop'
# Solo credenciales ficticias del emulador; Terraform rechaza endpoints que no sean localhost.
$env:AWS_ACCESS_KEY_ID = 'test'
$env:AWS_SECRET_ACCESS_KEY = 'test'
$env:AWS_DEFAULT_REGION = 'us-east-1'
$python = '.\.venv\Scripts\python.exe'

# Terraform 1.13 en tools\ o en el PATH.
$terraform = if (Test-Path '.\tools\terraform.exe') { (Resolve-Path '.\tools\terraform.exe').Path }
             elseif (Get-Command terraform -ErrorAction SilentlyContinue) { 'terraform' }
             else { throw 'Instale Terraform 1.13 o copie terraform.exe en tools\' }
function Invoke-Terraform([string[]]$argumentos) {
    & $terraform "-chdir=terraform" @argumentos
    if ($LASTEXITCODE -ne 0) { throw "terraform $($argumentos -join ' ') falló" }
}

docker compose up -d --wait
& $python scripts\empaquetar_lambda.py
Invoke-Terraform @('init', '-input=false')
Invoke-Terraform @('apply', '-auto-approve', '-input=false')
$env:AWS_ENDPOINT_URL = 'http://localhost:4566'
& $python -m pytest -q -m integracion
& $python scripts\medir_integracion.py
& $python scripts\generar_figuras.py
Write-Host 'Listo. Para liberar todo: docker compose down -v'
