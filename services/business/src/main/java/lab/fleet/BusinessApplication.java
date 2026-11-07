package lab.fleet;

import java.util.List;
import java.util.Map;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.bind.annotation.*;

@SpringBootApplication
@RestController
@RequestMapping("/business")
public class BusinessApplication {
    private final JdbcTemplate db;
    private final StringRedisTemplate cache;
    public BusinessApplication(JdbcTemplate db, StringRedisTemplate cache) {this.db=db;this.cache=cache;}
    public static void main(String[] args) {SpringApplication.run(BusinessApplication.class,args);}
    @GetMapping("/vehicles")
    public List<Map<String,Object>> vehicles(@RequestParam(defaultValue="SZ-01") String station) {
        return db.queryForList("SELECT * FROM vehicles WHERE station_id=? ORDER BY vehicle_id LIMIT 500",station);
    }
    @GetMapping("/tasks")
    public List<Map<String,Object>> tasks(@RequestParam(defaultValue="SZ-01") String station) {
        return db.queryForList("SELECT * FROM tasks WHERE station_id=? ORDER BY created_at DESC LIMIT 100",station);
    }
    @GetMapping("/routes/{vehicleId}")
    public Map<String,Object> route(@PathVariable String vehicleId) {
        String key="fleet:route:"+vehicleId;
        String value=cache.opsForValue().get(key);
        Long ttl=cache.getExpire(key);
        return Map.of("vehicle_id",vehicleId,"cached",value==null?"":value,"ttl_seconds",ttl==null?-2:ttl);
    }
}
