#!/usr/bin/env bash
print_title () {
	echo -e """
		\e[1m  \e[92m $1 \e[0m
	"""
}

../setup_env.sh
eval "$(conda shell.bash hook)"
conda activate edgar
echo "python=$(which python)"
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
export PYTHONPATH=$(dirname $DIR)

print_title "Creating database views"
python views.py

print_title "Cleaning filing indexes"
python cleaning.py index


#!/usr/bin/env bash
docker cp ./positions_materialized_view.js 13fmongo:/positions_materialized_view.js
docker exec 13fmongo sh -c 'mongo edgar /positions_materialized_view.js'

#print_title "Cleaning filing stock info"
#python cleaning.py info

#print_title "Cleaning filing positions"
#python cleaning.py positions

#print_title "Building positions collection"
##cores=$(($(nproc)-2))
#cores=10
##echo "Using number of cores=$cores"
#python update_positions.py --n_cores=$cores run