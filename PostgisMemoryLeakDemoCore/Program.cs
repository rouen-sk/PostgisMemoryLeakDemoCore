using Npgsql;
using System;
using System.Diagnostics;
using System.Globalization;
using System.Threading.Tasks;

namespace PostgisMemoryLeakDemoCore
{
    class Program
    {
        static void Main(string[] args)
        {
            var connBuilder = new NpgsqlConnectionStringBuilder()
            {
                Host = "",
                Port = 5432,
                Username = "",
                Password = "",
                Database = "",
                CommandTimeout = 600
            };
                        
            int workers_count = 7;

            // big chuck of Europe
            double lat_min = 47;
            double lat_max = 52;
            double lon_min = 5;
            double lon_max = 22;

            var connString = connBuilder.ToString();

            for (int w = 0; w < workers_count; w++)
            {
                Task.Run(() =>
                {
                    var conn = new NpgsqlConnection(connString);
                    conn.Open();

                    var cmd = conn.CreateCommand();

                    cmd.CommandText = "SELECT pg_backend_pid();";
                    var backendPid = (int)cmd.ExecuteScalar();

                    var random = new Random();
                    var sw = new Stopwatch();

                    while (true)
                    {
                        // get random coordinates inside Europe
                        var lat = (lat_max - lat_min) * random.NextDouble() + lat_min;
                        var lon = (lon_max - lon_min) * random.NextDouble() + lon_min;

                        var x_min = MercatorProjection.LonToX(lon);
                        var y_min = MercatorProjection.LatToY(lat);

                        var x_max = MercatorProjection.LonToX(lon + 0.2);
                        var y_max = MercatorProjection.LatToY(lat + 0.2);
                                               
                        string box = $"ST_SetSRID('BOX({x_min.ToString(CultureInfo.InvariantCulture)} {y_min.ToString(CultureInfo.InvariantCulture)},{x_max.ToString(CultureInfo.InvariantCulture)} {y_max.ToString(CultureInfo.InvariantCulture)})'::box2d, 3857)";

                        // polygon manipulation test - seems to be leaking slowly (~500 MB/hour with 7 workers)
                        //cmd.CommandText = "SELECT ST_SimplifyPreserveTopology(st_buffer(st_union(st_buffer(t.geometry, 4)), -4), 4) FROM osm_landuse t WHERE t.geometry && " + box;

                        // linestring manipulation test - leaking fast (6 GB/hour with 7 workers)
                        cmd.CommandText = "SELECT ST_SimplifyPreserveTopology(ST_LineMerge(ST_Collect(t.geometry)),4) FROM osm_road t WHERE t.geometry && " + box + " GROUP BY t.class, t.type, t.name;";

                        int rows = 0;

                        sw.Restart();

                        using (var reader = cmd.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                // discard results, just count rows
                                rows++;
                            }
                        }

                        sw.Stop();
                        Console.WriteLine($"Worker {backendPid} got {rows} rows in {sw.ElapsedMilliseconds}ms");
                    }
                });
            }

            Console.ReadLine();
        }
    }
}
