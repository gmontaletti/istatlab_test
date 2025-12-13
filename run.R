# run.R
# Script to execute the targets workflow
# Author: Giampaolo Montaletti

# 1. Load targets -----
library(targets)

# 2. Check pipeline status -----
message("Pipeline targets:")
print(tar_manifest())

message("\nOutdated targets:")
print(tar_outdated())

# 3. Execute pipeline -----
message("\nExecuting pipeline...")
tar_make()

# 4. View results -----
message("\nPipeline complete.")
