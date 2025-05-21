# plastic-backend
Backend for the marine-plastic-tracker application


 WITH cte AS (
    SELECT trajectory FROM '/homelocal/jgasperi/Devel/plastic-backend/data/test_geoparquet.parquet' 
    WHERE obs = 0
    AND ST_Intersects(geometry, ST_GeomFromText('POLYGON((-80.200195 30.372875, -81.782227 29.726222, -80.551758 26.431228, -79.101562 25.085599, -80.200195 30.372875))'))
 )
 SELECT p.trajectory, p.obs, ST_AsGeoJSON(p.geometry) AS geometry
 FROM '/homelocal/jgasperi/Devel/plastic-backend/data/test_geoparquet.parquet' p
 JOIN cte c
 ON c.trajectory = p.trajectory
 ORDER by p.trajectory, p.obs;

