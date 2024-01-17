#!/bin/bash
STORAGE_POOL_USAGE_PATH_FILE="storage_pool_usage_paths.txt"
STORAGE_USAGE_PATH_FILE="storage_usage_paths.txt"

echo "Generate the access zone overview panel that shows capacity usage by access zone"
python gen_access_zone_filter_ndjson.py templates/template_access_zone_overview.ndjson output/access_zone_overview.ndjson

echo "Generate the pie chart with capacity based on all access zones except the System zone"
python gen_access_zone_filter_no_system_ndjson.py templates/template_capacity_by_access_zone_no_system.ndjson output/capacity_by_access_zone_no_system.ndjson

echo "Generate the pie chart with file count based on all access zones except the System zone"
python gen_access_zone_filter_no_system_ndjson.py templates/template_file_count_by_access_zone_no_system.ndjson output/file_count_by_access_zone_no_system.ndjson

echo "Generate the storage overview based on the storage pools on the cluster"
python gen_storagepool_usage_ndjson.py templates/template_storagepool_usage.ndjson output/storagepool_usage.ndjson

if [ -f "${STORAGE_POOL_USAGE_PATH_FILE}" ];
  echo "Generating storage pool usage by path"
  python gen_storage_pool_usage_by_path_ndjson.py \
      templates/template_storage_pool_usage_by_path.ndjson \
      "${STORAGE_POOL_USAGE_PATH_FILE}" \
      output/storage_pool_usage_by_path.ndjson
else
  echo "Cannot generate storage pool usage by path"
  echo "Create a file called ${STORAGE_POOL_USAGE_PATH_FILE} with 1 path per line, starting with /ifs to create panel for storage pool usage by path"
fi

if [ -f "${STORAGE_USAGE_PATH_FILE}" ];
  echo "Generating storage usage by path"
  python gen_storage_usage_by_path_ndjson.py templates/template_storage_usage_by_path.ndjson "${STORAGE_USAGE_PATH_FILE}" output/storage_usage_by_path.ndjson
else
  echo "Cannot generate storage usage by path"
  echo "Create a file called ${STORAGE_USAGE_PATH_FILE} with 1 path per line, starting with /ifs to create panel for storage usage by path"
fi

echo "Generating file categories panel"
echo "To modify file extensions per category, modify the gen_file_category_ndjson.py file directly"
python gen_file_category_ndjson.py templates/template_file_categories.ndjson output/file_categories.ndjson

echo "Copying fixed panels to output directory"
echo "Copying DRR statistics panel"
cp templates/drr_statistics.ndjson output
echo "Copying orphaned files panel. Default reports on unresolvable UID/GID/SID and any user names that start with zzz"
cp templates/orphaned_files.ndjson output
echo "Copying open permissions panel. Default reports on any file/directory that has a UNIX world writable bit enabled"
cp templates/open_permissions.ndjson output
