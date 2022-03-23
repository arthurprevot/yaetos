# To be run manually to check deltas between files that should be very similar.

/usr/bin/opendiff Dockerfile Dockerfile_alt
/usr/bin/opendiff yaetos/scripts/setup_master.sh yaetos/scripts/setup_master_alt.sh
/usr/bin/opendiff yaetos/scripts/setup_node.sh yaetos/scripts/setup_node_alt.sh
