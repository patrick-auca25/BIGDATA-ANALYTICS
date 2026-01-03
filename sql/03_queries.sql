SELECT time_bucket('1 hour', timestamp) AS hour,  
       AVG(power) as avg_power 
FROM energy_readings 
WHERE timestamp >= DATE_TRUNC('day', NOW()) 
GROUP BY hour ORDER BY hour; 


SELECT time_bucket('15 minutes', timestamp) AS period,  
       AVG(power) as avg_power 
FROM energy_readings 
WHERE timestamp >= NOW() - INTERVAL '7 days' 
GROUP BY period ORDER BY avg_power DESC LIMIT 10;






    SELECT 
        meter_id,
        TO_CHAR(DATE_TRUNC('month', timestamp), 'YYYY-MM') as month,
        ROUND(SUM(energy)::numeric, 2) as total_energy
    FROM energy_readings
    GROUP BY meter_id, DATE_TRUNC('month', timestamp)
    ORDER BY DATE_TRUNC('month', timestamp), total_energy DESC;


SELECT COUNT(*), AVG(power), MAX(power), MIN(power) 
FROM energy_readings; 