#!/usr/bin/env bash
eval "$(conda shell.bash hook)"
ENVS=$(conda env list | awk '{print $1}' )
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
DIR="$(dirname "$DIR")"
if [[ $ENVS = *"edgar"* ]]; then
    echo "Activating env edgar"
    conda activate edgar
else
    echo "Creating conda env edgar..."
    conda create -y -n edgar python=3.7
    conda activate edgar
    pip install -r $DIR/requirements.txt
fi;

#echo "python=$(which python)"
