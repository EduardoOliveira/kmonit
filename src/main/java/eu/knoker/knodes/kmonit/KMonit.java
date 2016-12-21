package eu.knoker.knodes.kmonit;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;
import org.hyperic.sigar.FileSystem;
import org.hyperic.sigar.FileSystemUsage;
import org.hyperic.sigar.Sigar;
import org.hyperic.sigar.SigarException;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by eduardo on 20/12/2016.
 */
public class KMonit extends AbstractVerticle {

    private Sigar sigar = new Sigar();
    private EventBus eb;
    private List<FileSystem> fileSystemList;

    @Override
    public void start() throws Exception {
        super.start();
        this.eb = vertx.eventBus();
        registerFs();
    }

    private void registerFs() throws Exception {
        this.fileSystemList = Arrays.stream(this.sigar.getFileSystemList()).filter(fs ->
                fs.getType() == FileSystem.TYPE_LOCAL_DISK
        ).collect(Collectors.toList());
        eb.consumer("knodes.kmonit.get.fsList", ms -> {
            JsonObject obj = new JsonObject();
            obj.put("fsList", fileSystemList.stream().map(FileSystem::getDevName).collect(Collectors.toList()));
            eb.send("knodes.kmonit.set.fsList", obj);
        });
        vertx.setPeriodic(1000, this::fsUsage);
    }

    private void fsUsage(Long id) {
        try {
            for (FileSystem fs : fileSystemList) {
                FileSystemUsage usage = this.sigar.getFileSystemUsage(fs.getDirName());
                String devName = fs.getDevName();
                eb.send("knodes.kmonit.set.fsUsage." + devName, new JsonObject(usage.toMap()));
            }
        } catch (SigarException e) {
            e.printStackTrace();
        }
    }
}
