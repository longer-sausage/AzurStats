#!/bin/env bash

pprint() { # Pretty print
    echo -e "==> \e[36m${1}\e[39m"
}
prun() { # Pretty run
    pprint "$ ${1}"
    eval " ${1}" || exit 1
}
prun_or_continue() { # Pretty run or continue
    pprint "$ ${1}"
    eval " ${1}" || pprint "↳ Not needed!"
}

SOURCE="$(dirname $(realpath ${BASH_SOURCE}))" # Poiting to dev_tools folder
CONTAINER="azurstats"

pprint "Killing any previous container"
prun "docker ps | grep ${CONTAINER} | awk '{print \$1}' | xargs -r -n1 docker kill"
pprint "Deleting old containers"
prun "docker ps -a | grep ${CONTAINER} | awk '{print \$1}' | xargs -r -n1 docker rm"

pprint "Build the image"
prun "docker build -t ${CONTAINER} -f ${SOURCE}/Dockerfile ${SOURCE}/../.."

pprint "Running the container"
trap "docker kill ${CONTAINER}" EXIT
prun "docker run --net=host --volume=${SOURCE}/../..:/app/azurstats:rw --interactive --tty --name ${CONTAINER} -p 22240:22240 ${CONTAINER}"
