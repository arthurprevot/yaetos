# To be run manually to check deltas between files that should be very similar.

/usr/bin/opendiff Dockerfile Dockerfile_alt
/usr/bin/opendiff core/scripts/setup_master.sh core/scripts/setup_master_alt.sh
/usr/bin/opendiff core/scripts/setup_node.sh core/scripts/setup_node_alt.sh
