#!bash

printf -- '%s\0' jsonschema/pipes/*.schema.json | xargs -0 \
    quicktype -s schema -l rust \
        --visibility public --derive-debug --derive-clone --derive-partial-eq -o \
        src/types.rs
