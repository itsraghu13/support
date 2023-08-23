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
    
    $fileContent = ""
    try {
        $gzStream = New-Object IO.Compression.GZipStream (New-Object IO.FileStream $gzFile.FullName, [IO.Compression.CompressionMode]::Decompress)
        $reader = New-Object IO.StreamReader $gzStream
        $fileContent = $reader.ReadToEnd()
        $reader.Close()
        $gzStream.Close()
    } catch {
        Write-Host "Error decompressing $($gzFile.Name): $_"
    }

    if ($fileContent -match $searchPattern) {
        $matchingFiles += $gzFile
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
