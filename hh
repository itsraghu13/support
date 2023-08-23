# Define the folder path
$folderPath = "C:\path\to\your\folder"

# Define the search pattern
$searchPattern = "T\d{2}:\d{2}:\d{2}\.\d{3}$|.*\d{2}:\d{2}:\d{2}\.\d{3}Z$|T\d{2}:\d{2}:\d{2}\.\d{3}Z$"

# Get a list of .gz files in the folder
$gzFiles = Get-ChildItem -Path $folderPath -Filter *.gz

# Initialize an array to store matching files
$matchingFiles = @()

# Loop through each .gz file and check for the search pattern
foreach ($gzFile in $gzFiles) {
    Write-Host "Searching in $($gzFile.Name)"
    
    try {
        $extractedPath = Join-Path $folderPath $gzFile.BaseName
        [System.IO.Compression.ZipFile]::ExtractToDirectory($gzFile.FullName, $extractedPath)

        $extractedFiles = Get-ChildItem -Path $extractedPath -File
        foreach ($extractedFile in $extractedFiles) {
            $fileContent = Get-Content $extractedFile.FullName
            if ($fileContent -match $searchPattern) {
                $matchingFiles += $gzFile
                break
            }
        }

        Remove-Item -Path $extractedPath -Recurse -Force
    } catch {
        Write-Host "Error processing $($gzFile.Name): $_"
    }
}

if ($matchingFiles.Count -gt 0) {
    Write-Host "Files containing rows with the specified format:"
    foreach ($file in $matchingFiles) {
        Write-Host $file.FullName
    }
} else {
    Write-Host "No files containing rows with the specified format found."
}



















# Define the folder path
$folderPath = "C:\path\to\your\folder"

# Define the search pattern
$searchPattern = "T\d{2}:\d{2}:\d{2}\.\d{3}$|.*\d{2}:\d{2}:\d{2}\.\d{3}Z$|T\d{2}:\d{2}:\d{2}\.\d{3}Z$"

# Get a list of .gz files in the folder
$gzFiles = Get-ChildItem -Path $folderPath -Filter *.gz

# Initialize an array to store matching files
$matchingFiles = @()

# Loop through each .gz file and check for the search pattern
foreach ($gzFile in $gzFiles) {
    Write-Host "Searching in $($gzFile.Name)"
    
    try {
        $stream = [System.IO.File]::OpenRead($gzFile.FullName)
        $gzStream = New-Object System.IO.Compression.GZipStream($stream, [System.IO.Compression.CompressionMode]::Decompress)
        $reader = New-Object System.IO.StreamReader($gzStream)
        $fileContent = $reader.ReadToEnd()
        $reader.Close()
        $gzStream.Close()
        $stream.Close()

        if ($fileContent -match $searchPattern) {
            $matchingFiles += $gzFile
        }
    } catch {
        Write-Host "Error processing $($gzFile.Name): $_"
    }
}

if ($matchingFiles.Count -gt 0) {
    Write-Host "Files containing rows with the specified format:"
    foreach ($file in $matchingFiles) {
        Write-Host $file.FullName
    }
} else {
    Write-Host "No files containing rows with the specified format found."
}




Install-Module -Name System.IO.Compression.FileSystem

