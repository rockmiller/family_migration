$projectRoot = "C:\Users\rockm\Documents\family_migration"

$folders = @(
    "$projectRoot\common\data",
    "$projectRoot\common\notebooks",
    "$projectRoot\tests",
    "$projectRoot\census_database\sql",
    "$projectRoot\census_database\scripts",
    "$projectRoot\census_database\notebooks",
    "$projectRoot\census_database\docs",
    "$projectRoot\study_population\sql",
    "$projectRoot\study_population\scripts",
    "$projectRoot\study_population\notebooks",
    "$projectRoot\study_population\docs",
    "$projectRoot\family_processing\dataclasses",
    "$projectRoot\family_processing\scripts",
    "$projectRoot\family_processing\notebooks",
    "$projectRoot\family_processing\docs",
    "$projectRoot\kinship_graphs\scripts",
    "$projectRoot\kinship_graphs\graphs",
    "$projectRoot\kinship_graphs\notebooks",
    "$projectRoot\kinship_graphs\docs",
    "$projectRoot\migration_analysis\scripts",
    "$projectRoot\migration_analysis\notebooks",
    "$projectRoot\migration_analysis\docs"
)

foreach ($folder in $folders) {
    New-Item -ItemType Directory -Path $folder -Force | Out-Null
}

# Create top-level placeholder files
New-Item -Path "$projectRoot\.gitignore" -ItemType File -Force | Out-Null
New-Item -Path "$projectRoot\README.md" -ItemType File -Force | Out-Null
New-Item -Path "$projectRoot\pyproject.toml" -ItemType File -Force | Out-Null

# Create empty test files
$testFiles = @(
    "conftest.py",
    "test_census_database.py",
    "test_study_population.py",
    "test_family_processing.py",
    "test_kinship_graphs.py",
    "test_migration_analysis.py",
    "test_common.py"
)

foreach ($file in $testFiles) {
    New-Item -Path "$projectRoot\tests\$file" -ItemType File -Force | Out-Null
}

Write-Host "Project structure created at $projectRoot"

