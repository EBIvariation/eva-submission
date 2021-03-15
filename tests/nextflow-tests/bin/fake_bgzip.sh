#!/bin/bash

# bgzip command eats stdout
>&2 echo "bgzip $*"
