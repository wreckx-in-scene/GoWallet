$services = "wallet", "fraud", "payment", "ledger", "auth", "user", "gateway" "notification"
foreach ($s in $services) {
    Start-Process powershell -ArgumentList "-NoExit", "-Command", "go run ./cmd/$s"
}