# The implementationis to migration Ceph cluster customer data to Rook.
# Steps as following:
# 1. In original Ceph cluster, scan the pools and get pool information
# 2. For each pool, export the pool content by rados:
#      rados -p <pool_name> export <pool_filename>
# 3. In Rook cluster, for each pool, create pool with the original information.
# 4. After the pool creation, import pool_filename into Rook cluster:
#      rados -p <pool_name> import <pool_filename> 

import rados, sys, getopt, json

class CephPoolMeta(object):
    """Class to describe Pool information"""
    def __init__(self):
        self.pool_name = ''
        self.pg_num = None
        self.pgp_num = None
        self.pool_type = None

class RadosOperator(object):
    """Class to encapsulate Rados python client for Rados object migration."""

    def __init__(self, export_config_file, import_config_file):
        self.export_config_file = export_config_file
        self.import_config_file = import_config_file
        self.export_cluster = None
        self.import_cluster = None
        self.pools = []

    def _load_and_connect_cluster(self, cluster_config_file):
        try:
            cluster = rados.Rados(conffile=cluster_config_file)
        except Exception as e:
            print("Argument validation error: ", e)
            raise e

        print("Created cluster handle by %s."%cluster_config_file)

        try:
            cluster.connect()
        except Exception as e:
            print("Cluster connection error: ", e)
            raise e
        finally:
            print("Connected to the cluster.")
            return cluster

    def initial_clusters(self):
        self.export_cluster = \
            self._load_and_connect_cluster(self.export_config_file)
        self.import_cluster = \
            self._load_and_connect_cluster(self.import_config_file)

    def shutdown_clusters(self):
        print("Shutting down the export cluster.")
        self.export_cluster.shutdown()
        print("Shutting down the import cluster.")
        self.import_cluster.shutdown()

    def _scan_pools(self, ceph_cluster):

        if ceph_cluster is None:
            print("Ceph cluster is None.")

        pools = ceph_cluster.list_pools()
        for pool in pools:
            self.pools.append(pool)
        print("Ceph pools list", self.pools)

    def _copy_objects(self, pool_name):
        export_ioctx = self.export_cluster.open_ioctx(pool_name)
        import_ioctx = self.import_cluster.open_ioctx(pool_name)
        
        object_iterator = export_ioctx.list_objects()
        while True:
            try:
                rados_object = object_iterator.next()
                print("Object: %s "%str(rados_object))
                psize, _ = rados_object.stat()
                xattrs_iter = rados_object.get_xattrs()
                import_ioctx.write_full(rados_object.key,
                    rados_object.read(length=psize))

                while True:
                    try:
                        name, val = xattrs_iter.next()
                        import_ioctx.set_xattr(rados_object.key, name, val)
                    except StopIteration:
                        break
            except StopIteration:
                break

        export_ioctx.close()
        import_ioctx.close()

    def bulk_export_pools(self):
        self._scan_pools(self.export_cluster)
        for pool in self.pools:
            self.import_cluster.create_pool(pool)
            self._copy_objects(pool)


def main(argv):
    export_cluster_config_file = ''
    import_cluster_config_file = ''
    try:
        opts, args = getopt.getopt(argv, "he:i:", ["export=", "import="])
    except getopt.GetoptError:
        print 'rados_migration.py -e <export_config_file> -i <import_config_file>'
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print 'rados_migration.py -e <export_config_file> -i <import_config_file>'
            sys.exit()
        elif opt in ("-e", "--export_config_file"):
            export_cluster_config_file = arg
        elif opt in ("-i", "--import_config_file"):
            import_cluster_config_file = arg

    rados_op = RadosOperator(export_cluster_config_file, import_cluster_config_file)
    rados_op.initial_clusters()
    rados_op.bulk_export_pools()
    rados_op.shutdown_clusters()

if __name__ == '__main__':
    main(sys.argv[1:])
    

