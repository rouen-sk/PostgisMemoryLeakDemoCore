﻿using System;
using System.Collections.Generic;
using System.Text;

namespace PostgisMemoryLeakDemoCore
{
    public class Point
    {
        public double X { get; set; }
        public double Y { get; set; }
    }

    public static class MercatorProjection
    {
        private static readonly double R_MAJOR = 6378137.0;
        private static readonly double R_MINOR = 6378137.0;// 6356752.3142;
        private static readonly double RATIO = R_MINOR / R_MAJOR;
        private static readonly double ECCENT = Math.Sqrt(1.0 - (RATIO * RATIO));
        private static readonly double COM = 0.5 * ECCENT;

        private static readonly double DEG2RAD = Math.PI / 180.0;
        private static readonly double RAD2Deg = 180.0 / Math.PI;
        private static readonly double PI_2 = Math.PI / 2.0;

        public static Point ToPixel(double lon, double lat)
        {
            return new Point { X = LonToX(lon), Y = LatToY(lat) };
        }

        public static Point ToGeoCoord(double x, double y)
        {
            return new Point { X = XToLon(x), Y = YToLat(y) };
        }

        public static double LonToX(double lon)
        {
            return R_MAJOR * DegToRad(lon);
        }

        public static double LatToY(double lat)
        {
            lat = Math.Min(89.5, Math.Max(lat, -89.5));
            double phi = DegToRad(lat);
            double sinphi = Math.Sin(phi);
            double con = ECCENT * sinphi;
            con = Math.Pow(((1.0 - con) / (1.0 + con)), COM);
            double ts = Math.Tan(0.5 * ((Math.PI * 0.5) - phi)) / con;
            return 0 - R_MAJOR * Math.Log(ts);
        }

        public static double XToLon(double x)
        {
            return RadToDeg(x) / R_MAJOR;
        }

        public static double YToLat(double y)
        {
            double ts = Math.Exp(-y / R_MAJOR);
            double phi = PI_2 - 2 * Math.Atan(ts);
            double dphi = 1.0;
            int i = 0;
            while ((Math.Abs(dphi) > 0.000000001) && (i < 15))
            {
                double con = ECCENT * Math.Sin(phi);
                dphi = PI_2 - 2 * Math.Atan(ts * Math.Pow((1.0 - con) / (1.0 + con), COM)) - phi;
                phi += dphi;
                i++;
            }
            return RadToDeg(phi);
        }

        private static double RadToDeg(double rad)
        {
            return rad * RAD2Deg;
        }

        private static double DegToRad(double deg)
        {
            return deg * DEG2RAD;
        }
    }
}
