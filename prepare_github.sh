#!/bin/bash

# Lending Club Pipeline - GitHub Preparation Script
# This script prepares the repository for GitHub by cleaning up unnecessary files

echo "========================================="
echo "GitHub Repository Preparation"
echo "========================================="
echo ""

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Step 1: Clean up generated files
echo -e "${YELLOW}Step 1: Cleaning up generated files...${NC}"
rm -rf data/outputs/*
rm -rf data/duckdb/*
rm -rf dagster_home/*
rm -rf dbt_project/target/*
rm -rf dbt_project/logs/*
rm -rf dbt_project/dbt_packages/*
rm -rf __pycache__
find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null
find . -type f -name "*.pyc" -delete
find . -type f -name "*.pyo" -delete
find . -type f -name "*.log" -delete
find . -type f -name ".DS_Store" -delete
echo -e "${GREEN}✓ Cleaned up generated files${NC}"
echo ""

# Step 2: Remove temporary and test files
echo -e "${YELLOW}Step 2: Removing temporary files...${NC}"
rm -f test_databricks_asset.py
rm -f TASK_*.md
rm -f TEST_RESULTS.md
rm -f VALIDATION_GUIDE.md
rm -f DOCKER_SETUP.md
rm -f lc-pipeline-v1.code-workspace
echo -e "${GREEN}✓ Removed temporary files${NC}"
echo ""

# Step 3: Rename README for GitHub
echo -e "${YELLOW}Step 3: Preparing README...${NC}"
if [ -f "README_GITHUB.md" ]; then
    cp README.md README_ORIGINAL.md.bak
    cp README_GITHUB.md README.md
    echo -e "${GREEN}✓ README prepared for GitHub${NC}"
else
    echo -e "${GREEN}✓ README already configured${NC}"
fi
echo ""

# Step 4: Verify essential files
echo -e "${YELLOW}Step 4: Verifying essential files...${NC}"
essential_files=(
    "README.md"
    "LICENSE"
    ".gitignore"
    ".env.example"
    "requirements.txt"
    "pyproject.toml"
    "docker-compose.yml"
    "Dockerfile"
    "workspace.yaml"
    "dagster.yaml"
)

all_present=true
for file in "${essential_files[@]}"; do
    if [ -f "$file" ]; then
        echo -e "${GREEN}✓${NC} $file"
    else
        echo -e "${YELLOW}✗${NC} $file (missing)"
        all_present=false
    fi
done
echo ""

# Step 5: Check directory structure
echo -e "${YELLOW}Step 5: Verifying directory structure...${NC}"
essential_dirs=(
    "src/lending_club_pipeline"
    "dbt_project/models"
    "tests"
    "data/inputs"
    "docs"
)

for dir in "${essential_dirs[@]}"; do
    if [ -d "$dir" ]; then
        echo -e "${GREEN}✓${NC} $dir/"
    else
        echo -e "${YELLOW}✗${NC} $dir/ (missing)"
        all_present=false
    fi
done
echo ""

# Step 6: Create .gitkeep files for empty directories
echo -e "${YELLOW}Step 6: Creating .gitkeep files...${NC}"
touch data/inputs/.gitkeep
touch data/outputs/.gitkeep
touch data/duckdb/.gitkeep
echo -e "${GREEN}✓ Created .gitkeep files${NC}"
echo ""

# Step 7: Summary
echo "========================================="
echo "Summary"
echo "========================================="
echo ""

if [ "$all_present" = true ]; then
    echo -e "${GREEN}✓ All essential files and directories present${NC}"
else
    echo -e "${YELLOW}⚠ Some files or directories are missing${NC}"
fi

echo ""
echo "Files to commit:"
echo "  - Source code (src/)"
echo "  - DBT project (dbt_project/)"
echo "  - Tests (tests/)"
echo "  - Documentation (docs/ + *.md files)"
echo "  - Configuration (.env.example, docker-compose.yml, etc.)"
echo "  - Data structure (data/ with .gitkeep files)"
echo ""

echo "Files excluded (via .gitignore):"
echo "  - .env (contains secrets)"
echo "  - venv/ (virtual environment)"
echo "  - data/outputs/* (generated files)"
echo "  - data/duckdb/* (database files)"
echo "  - dagster_home/* (runtime files)"
echo "  - __pycache__/ (Python cache)"
echo "  - .kiro/ (IDE files)"
echo ""

echo "========================================="
echo "Next Steps:"
echo "========================================="
echo ""
echo "1. Review changes:"
echo "   git status"
echo ""
echo "2. Initialize git (if not already done):"
echo "   git init"
echo ""
echo "3. Add files:"
echo "   git add ."
echo ""
echo "4. Commit:"
echo "   git commit -m 'Initial commit: Lending Club Pipeline'"
echo ""
echo "5. Create GitHub repository and push:"
echo "   git remote add origin https://github.com/YOUR_USERNAME/lending-club-pipeline.git"
echo "   git branch -M main"
echo "   git push -u origin main"
echo ""
echo -e "${GREEN}✓ Repository prepared for GitHub!${NC}"
echo ""
