import os
import re
import random

folder_path = r"C:\path\to\your\folder"
search_pattern = r'.*(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z|\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}|\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}Z).*'
sample_size = 2

matching_files = []

for file_name in os.listdir(folder_path):
    if file_name.endswith(".txt"):
        file_path = os.path.join(folder_path, file_name)
        print(f"Processing {file_path}")

        with open(file_path, "r") as file:
            sampled_lines = random.sample(file.readlines(), sample_size)
            for line in sampled_lines:
                if re.match(search_pattern, line):
                    print(f"Matching line found in {file_path}:")
                    print(line.strip())
                    matching_files.append(file_path)
                    break

if matching_files:
    print("Files containing matching lines:")
    for file_path in matching_files:
        print(file_path)
else:
    print("No files containing matching lines found.")

























'".*(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z|\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}|\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}Z).*"'

$folderPath = "$PSHOME\en-US\TimeLogs"
$searchPattern = '"(?<time>(?:YYYY-MM-DDT|YYYY-MM-DD )\d{2}:\d{2}:\d{2}\.\d{3}(Z|))"'

$matchingFiles = @()

Get-ChildItem -Path $folderPath -Filter *.txt | ForEach-Object {
    $filePath = $_.FullName
    Write-Host "Searching in $($filePath)"

    $fileContent = Get-Content $filePath
    $matchingLines = $fileContent | Where-Object { $_ -match $searchPattern }
    
    if ($matchingLines.Count -gt 0) {
        Write-Host "Matching line(s) found in $($filePath):"
        $matchingLines | ForEach-Object {
            Write-Host $_
        }
        $matchingFiles += $_
    }
}

if ($matchingFiles.Count -gt 0) {
    Write-Host "Files containing matching lines:"
    $matchingFiles | ForEach-Object {
        Write-Host $_.FullName
    }
} else {
    Write-Host "No files containing matching lines found."
}












$folderPath = "$PSHOME\en-US\TimeLogs"
$searchPattern = '(?<time>(?:YYYY-MM-DDT|YYYY-MM-DD )\d{2}:\d{2}:\d{2}\.\d{3}(Z|))'

$matchingFiles = @()

Get-ChildItem -Path $folderPath -Filter *.txt | ForEach-Object {
    $filePath = $_.FullName
    Write-Host "Searching in $($filePath)"

    $fileContent = Get-Content $filePath
    $matchingLines = $fileContent | Where-Object { $_ -match $searchPattern }
    
    if ($matchingLines.Count -gt 0) {
        Write-Host "Matching line(s) found in $($filePath):"
        $matchingLines | ForEach-Object {
            Write-Host $_
        }
        $matchingFiles += $_
    }
}

if ($matchingFiles.Count -gt 0) {
    Write-Host "Files containing matching lines:"
    $matchingFiles | ForEach-Object {
        Write-Host $_.FullName
    }
} else {
    Write-Host "No files containing matching lines found."
}










'(?<time>(?:YYYY-MM-DDT|YYYY-MM-DD )\d{2}:\d{2}:\d{2}\.\d{3}(Z|))'
 @('YYYY-MM-DDT00:00:00.000Z', 'YYYY-MM-DDT00:00:00.000', 'YYYY-MM-DD 00:00:00.000Z')

'(T\d{2}:\d{2}:\d{2}\.\d{3}(?!Z)|.*\d{2}:\d{2}:\d{2}\.\d{3}|T\d{2}:\d{2}:\d{2}\.\d{3})'
$folderPath = "$PSHOME\en-US\TimeLogs"
$searchPattern = '.*\b(T\d{2}:\d{2}:\d{2}\.\d{3}Z|(?<time>T\d{2}:\d{2}:\d{2}\.\d{3})|.*\d{2}:\d{2}:\d{2}\.\d{3}Z)\b.*'

$matchingFiles = @()

Get-ChildItem -Path $folderPath -Filter *.txt | ForEach-Object {
    $filePath = $_.FullName
    Write-Host "Searching in $($filePath)"

    $fileContent = Get-Content $filePath
    $matchingLines = $fileContent | Where-Object { $_ -match $searchPattern -and $_ -notmatch "Time Zone" }
    
    if ($matchingLines.Count -gt 0) {
        Write-Host "Matching line(s) found in $($filePath):"
        $matchingLines | ForEach-Object {
            Write-Host $_
        }
        $matchingFiles += $_
    }
}

if ($matchingFiles.Count -gt 0) {
    Write-Host "Files containing matching lines:"
    $matchingFiles | ForEach-Object {
        Write-Host $_.FullName
    }
} else {
    Write-Host "No files containing matching lines found."
}












'.*\b(T\d{2}:\d{2}:\d{2}\.\d{3}\Z|\\T\d{2}:\d{2}:\d{2}\.\d{3}|\\.*\d{2}:\d{2}:\d{2}\.\d{3}\Z)\b.*'


    '.*\b(T\d{2}:\d{2}:\d{2}\.\d{3}Z|(?<time>T\d{2}:\d{2}:\d{2}\.\d{3})|.*\d{2}:\d{2}:\d{2}\.\d{3}Z)\b.*'




$folderPath = "$PSHOME\en-US"
$searchPattern = '.*\b(T\d{2}:\d{2}:\d{2}\.\d{3}Z|T\d{2}:\d{2}:\d{2}\.\d{3}|.*\d{2}:\d{2}:\d{2}\.\d{3}Z)\b.*'

$matchingFiles = @()

Get-ChildItem -Path $folderPath -Filter *.txt | ForEach-Object {
    $filePath = $_.FullName
    Write-Host "Searching in $($filePath)"

    $fileContent = Get-Content $filePath
    $matchingLines = $fileContent | Where-Object { $_ -match $searchPattern }
    
    if ($matchingLines.Count -gt 0) {
        Write-Host "Matching line(s) found in $($filePath):"
        $matchingLines | ForEach-Object {
            Write-Host $_
        }
        $matchingFiles += $_
    }
}

if ($matchingFiles.Count -gt 0) {
    Write-Host "Files containing matching lines:"
    $matchingFiles | ForEach-Object {
        Write-Host $_.FullName
    }
} else {
    Write-Host "No files containing matching lines found."
}




















'^.*\b(T\d{2}:\d{2}:\d{2}\.\d{3}Z|T\d{2}:\d{2}:\d{2}\.\d{3}|.*\d{2}:\d{2}:\d{2}\.\d{3}Z)\b.*$'



'^.*\bT\d{2}:\d{2}:\d{2}\.\d{3}Z\b|\bT\d{2}:\d{2}:\d{2}\.\d{3}\b|\b\d{2}:\d{2}:\d{2}\.\d{3}Z\b.*$'


$folderPath = "$PSHOME\en-US"
$searchPattern = 'T\d{2}:\d{2}:\d{2}\.\d{3}Z'

$matchingFiles = @()

Get-ChildItem -Path $folderPath -Filter *.txt | ForEach-Object {
    $filePath = $_.FullName
    Write-Host "Searching in $($filePath)"

    $fileContent = Get-Content $filePath
    $matchingLines = $fileContent | Where-Object { $_ -match $searchPattern }
    
    if ($matchingLines.Count -gt 0) {
        Write-Host "Matching line(s) found in $($filePath):"
        $matchingLines | ForEach-Object {
            Write-Host $_
        }
        $matchingFiles += $_
    }
}

if ($matchingFiles.Count -gt 0) {
    Write-Host "Files containing matching lines:"
    $matchingFiles | ForEach-Object {
        Write-Host $_.FullName
    }
} else {
    Write-Host "No files containing matching lines found."
}



$folderPath = "$PSHOME\en-US"
$searchPattern = '^.*T\d{2}:\d{2}:\d{2}\.\d{3}Z.*$|^.*T\d{2}:\d{2}:\d{2}\.\d{3}$|^.*\d{2}:\d{2}:\d{2}\.\d{3}Z.*$'

$matchingFiles = @()

Get-ChildItem -Path $folderPath -Filter *.txt | ForEach-Object {
    $filePath = $_.FullName
    Write-Host "Searching in $($filePath)"

    $fileContent = Get-Content $filePath
    $matchingLines = $fileContent | Where-Object { $_ -match $searchPattern }
    
    if ($matchingLines.Count -gt 0) {
        Write-Host "Matching line(s) found in $($filePath):"
        $matchingLines | ForEach-Object {
            Write-Host $_
        }
        $matchingFiles += $_
    }
}

if ($matchingFiles.Count -gt 0) {
    Write-Host "Files containing matching lines:"
    $matchingFiles | ForEach-Object {
        Write-Host $_.FullName
    }
} else {
    Write-Host "No files containing matching lines found."
}





Select-String -Path "$PSHOME\en-US\*.txt" -Pattern '^(T\d{2}:\d{2}:\d{2}\.\d{3}|.*\d{2}:\d{2}:\d{2}\.\d{3}Z|T\d{2}:\d{2}:\d{2}\.\d{3}Z)$'''



$folderPath = "$PSHOME\en-US"
$searchPattern = '^(T\d{2}:\d{2}:\d{2}\.\d{3}|.*\d{2}:\d{2}:\d{2}\.\d{3}Z|T\d{2}:\d{2}:\d{2}\.\d{3}Z)$'

$matchingFiles = @()

Get-ChildItem -Path $folderPath -Filter *.txt | ForEach-Object {
    $filePath = $_.FullName
    Write-Host "Searching in $($filePath)"

    $fileContent = Get-Content $filePath
    $matchingLines = $fileContent | Where-Object { $_ -match $searchPattern }
    
    if ($matchingLines.Count -gt 0) {
        Write-Host "Matching line found in $($filePath):"
        $matchingLines | Select-Object -First 1
        $matchingFiles += $_
    }
}

if ($matchingFiles.Count -gt 0) {
    Write-Host "Files containing matching lines:"
    $matchingFiles | ForEach-Object {
        Write-Host $_.FullName
    }
} else {
    Write-Host "No files containing matching lines found."
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
        $fileContent = Get-Content -Path $gzFile.FullName -Raw
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
        # Use a chunk size of 1 MB
        $chunkSize = 1MB
        
        # Get the file size
        $fileSize = $gzFile.Length
        
        # Read the file in chunks
        for ($offset = 0; $offset -lt $fileSize; $offset += $chunkSize) {
            $chunk = Get-Content -Path $gzFile.FullName -ReadCount $chunkSize -Raw
            
            if ($chunk -match $searchPattern) {
                $matchingFiles += $gzFile
            }
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
        # Use a chunk size of 1 MB
        $chunkSize = 1MB
        
        # Get the file size
        $fileSize = $gzFile.Length
        
        # Read the file in chunks
        for ($offset = 0; $offset -lt $fileSize; $offset += $chunkSize) {
            $chunk = Get-Content -Path $gzFile.FullName -ReadCount $chunkSize -Raw
            
            if ($chunk -match $searchPattern) {
                $matchingFiles += $gzFile
            }
        }
    } catch {
        Write-Host "Error processing $($gzFile.Name): $_"
    }
}

# Get the top 10000 matching files
$matchingFiles = $matchingFiles | Where-Object {$_ -le 10000}

if ($matchingFiles.Count -gt 0) {
    Write-Host "Files containing rows with the specified format:"
    foreach ($file in $matchingFiles) {
        Write-Host $file.FullName
    }
} else {
    Write-Host "No files containing rows with the specified format found."
}

