$ErrorActionPreference = "Stop"

$base  = "http://localhost:3000/api"
$email = "abdoelassali66@gmail.com"
$pass  = "Abdo123456789@"
$dbName = "Warehouse"

# 1) Login
Write-Host "[1/6] Logging in..."
$session = Invoke-RestMethod -Method Post -Uri "$base/session" `
  -ContentType "application/json" `
  -Body (@{ username = $email; password = $pass } | ConvertTo-Json)
$headers = @{ "X-Metabase-Session" = $session.id }
Write-Host "      OK"

# 2) Find the database id
Write-Host "[2/6] Looking up database '$dbName'..."
$dbs = Invoke-RestMethod -Method Get -Uri "$base/database" -Headers $headers
$db = $dbs.data | Where-Object { $_.name -eq $dbName }
if (-not $db) { throw "Database '$dbName' not found in Metabase" }
$dbId = $db[-1].id
Write-Host "      database id = $dbId"

# 3) Trigger sync + rescan
Write-Host "[3/6] Syncing schema and field values..."
Invoke-RestMethod -Method Post -Uri "$base/database/$dbId/sync_schema" -Headers $headers | Out-Null
Invoke-RestMethod -Method Post -Uri "$base/database/$dbId/rescan_values" -Headers $headers | Out-Null
Start-Sleep -Seconds 5
Write-Host "      sync triggered"

# 4) Create 3 SQL "questions" (cards)
function New-Question($name, $sql, $display) {
    $body = @{
        name = $name
        display = $display
        visualization_settings = @{}
        dataset_query = @{
            type = "native"
            database = $dbId
            native = @{ query = $sql }
        }
    } | ConvertTo-Json -Depth 10
    return Invoke-RestMethod -Method Post -Uri "$base/card" -Headers $headers `
        -ContentType "application/json" -Body $body
}

Write-Host "[4/6] Creating questions..."
$q1 = New-Question "Mots cles les plus frequents" @"
SELECT keyword, SUM(n_occurrences) AS n_occurrences
FROM gold.top_keywords
GROUP BY keyword
ORDER BY n_occurrences DESC
LIMIT 50;
"@ "bar"
Write-Host "      Q1 id=$($q1.id)"

$q2 = New-Question "Articles par source" @"
SELECT source, n_articles
FROM gold.articles_per_source
ORDER BY n_articles DESC;
"@ "bar"
Write-Host "      Q2 id=$($q2.id)"

$q3 = New-Question "Tendances d'actualite (7j)" @"
SELECT keyword, SUM(n_occurrences) AS total_occurrences
FROM gold.top_keywords
WHERE day >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY keyword
ORDER BY total_occurrences DESC
LIMIT 25;
"@ "bar"
Write-Host "      Q3 id=$($q3.id)"

$q4 = New-Question "Articles - dernieres 24h" @"
SELECT COUNT(*) AS articles_last_24h
FROM gold.articles
WHERE published_at >= NOW() - INTERVAL '24 hours';
"@ "scalar"
Write-Host "      Q4 id=$($q4.id)"

$q5 = New-Question "Total articles" @"
SELECT COUNT(*) AS total_articles FROM gold.articles;
"@ "scalar"
Write-Host "      Q5 id=$($q5.id)"

$q6 = New-Question "Latest articles" @"
SELECT published_at, source, language, title
FROM gold.articles
ORDER BY published_at DESC NULLS LAST
LIMIT 50;
"@ "table"
Write-Host "      Q6 id=$($q6.id)"

# 5) Create dashboard
Write-Host "[5/6] Creating dashboard..."
$dash = Invoke-RestMethod -Method Post -Uri "$base/dashboard" -Headers $headers `
  -ContentType "application/json" `
  -Body (@{ name = "NewsLake - Demo Jury"; description = "Dashboards generes automatiquement" } | ConvertTo-Json)
$dashId = $dash.id
Write-Host "      dashboard id = $dashId"

# 6) Add cards to the dashboard via the unified PUT /api/dashboard/:id endpoint
Write-Host "[6/6] Adding cards to dashboard..."
$cards = @(
    @{ id=-1; card_id=$q5.id; row=0;  col=0;  size_x=6;  size_y=3; visualization_settings=@{} },
    @{ id=-2; card_id=$q4.id; row=0;  col=6;  size_x=6;  size_y=3; visualization_settings=@{} },
    @{ id=-3; card_id=$q2.id; row=0;  col=12; size_x=12; size_y=6; visualization_settings=@{} },
    @{ id=-4; card_id=$q3.id; row=3;  col=0;  size_x=12; size_y=6; visualization_settings=@{} },
    @{ id=-5; card_id=$q1.id; row=9;  col=0;  size_x=24; size_y=8; visualization_settings=@{} },
    @{ id=-6; card_id=$q6.id; row=17; col=0;  size_x=24; size_y=8; visualization_settings=@{} }
)
$body = @{ dashcards = $cards } | ConvertTo-Json -Depth 10
Invoke-RestMethod -Method Put -Uri "$base/dashboard/$dashId" -Headers $headers `
  -ContentType "application/json" -Body $body | Out-Null

Write-Host ""
Write-Host "================================================================"
Write-Host " DONE !  Open http://localhost:3000/dashboard/$dashId"
Write-Host "================================================================"
