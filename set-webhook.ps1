# Get bot token from wrangler secrets
$token = Read-Host "Enter your Telegram Bot Token"
$webhookUrl = "https://telegram-bot.0xflintz.workers.dev"

# Delete existing webhook
Write-Host "Deleting existing webhook..."
$deleteUrl = "https://api.telegram.org/bot$token/deleteWebhook"
Invoke-RestMethod -Uri $deleteUrl -Method Post

Start-Sleep -Seconds 2

# Set new webhook
Write-Host "Setting new webhook to: $webhookUrl"
$setUrl = "https://api.telegram.org/bot$token/setWebhook?url=$webhookUrl"
$result = Invoke-RestMethod -Uri $setUrl -Method Post

Write-Host "Result: $($result | ConvertTo-Json)"

# Verify webhook
Write-Host "`nVerifying webhook..."
$infoUrl = "https://api.telegram.org/bot$token/getWebhookInfo"
$info = Invoke-RestMethod -Uri $infoUrl -Method Get
Write-Host "Webhook Info: $($info | ConvertTo-Json -Depth 3)"
