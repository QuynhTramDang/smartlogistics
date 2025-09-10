#!/bin/bash

set -e

# Tạo thư mục lưu dữ liệu nếu chưa có
mkdir -p osrm-data
cd osrm-data

# Danh sách các bang và file pbf tương ứng
declare -A STATES=(
  ["new-york"]="new-york-latest.osm.pbf"
  ["illinois"]="illinois-latest.osm.pbf"
  ["florida"]="florida-latest.osm.pbf"
  ["georgia"]="georgia-latest.osm.pbf"
  ["tennessee"]="tennessee-latest.osm.pbf"
  ["wisconsin"]="wisconsin-latest.osm.pbf"
  ["massachusetts"]="massachusetts-latest.osm.pbf"
  ["michigan"]="michigan-latest.osm.pbf"
  ["minnesota"]="minnesota-latest.osm.pbf"
  ["maryland"]="maryland-latest.osm.pbf"
  ["ohio"]="ohio-latest.osm.pbf"
  ["maine"]="maine-latest.osm.pbf"
  ["south-carolina"]="south-carolina-latest.osm.pbf"
  ["iowa"]="iowa-latest.osm.pbf"
  ["delaware"]="delaware-latest.osm.pbf"
)

BASE_URL="https://download.geofabrik.de/north-america/us"

# Tải từng file
for state in "${!STATES[@]}"; do
  file="${STATES[$state]}"
  url="${BASE_URL}/${file}"

  if [ -f "$file" ]; then
    echo " File $file đã tồn tại, bỏ qua."
  else
    echo "  Đang tải $file từ $url ..."
    wget -q --show-progress "$url" -O "$file"
    echo " Đã tải xong: $file"
  fi
done

echo " Hoàn tất. Các file .osm.pbf đã nằm trong thư mục osrm-data/"
