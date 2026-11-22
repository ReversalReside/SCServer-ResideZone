package szserver.inventory;

import szserver.net.InventoryPackets;
import java.io.*;
import java.nio.file.*;
import java.util.*;

/** Sorting is a durable server mutation driven by native packet 1506. */
public final class InventorySortTests {
    private static int checks;
    private static void check(boolean ok,String why){if(!ok)throw new AssertionError(why);checks++;}
    private static void rejected(Runnable operation){try{operation.run();throw new AssertionError("invalid sort request accepted");}catch(IllegalArgumentException expected){checks++;}}
    private static Inventory.Address address(int aggregate,int container,int slot){return new Inventory.Address(aggregate,container,slot);}
    private static int count(Inventory inventory,int aggregate,int container,int id){return inventory.items().entrySet().stream().filter(e->e.getKey().aggregate()==aggregate&&e.getKey().container()==container&&e.getValue().id()==id).mapToInt(e->e.getValue().count()).sum();}
    private static byte[] sortBody(int identity,int aggregate,int container,int slot)throws IOException{var out=new ByteArrayOutputStream();try(var data=new DataOutputStream(out)){data.writeShort(identity);data.writeShort(-1);data.writeByte(aggregate);data.writeByte(container);data.writeShort(slot);}return out.toByteArray();}
    public static void main(String[] args)throws Exception{
        Path root=Path.of(args[0]);Files.createDirectories(root);Path definitions=root.resolve("item-definitions.json");
        Files.writeString(definitions,"{\"1\":{\"type\":\"weapon\",\"slot_size\":\"2x1\"},\"2\":{\"type\":\"bullet\",\"stack_size\":10},\"3\":{\"type\":\"medicine\"},\"4\":{\"type\":\"armor\",\"slot_size\":\"2x3\"},\"5\":{\"type\":\"custom\"},\"6\":{\"type\":\"backpack\",\"slot_size\":\"2x2\"}}");
        ItemCatalog catalog=new ItemCatalog(definitions);
        Map<Inventory.Address,ItemStack> scattered=new LinkedHashMap<>();
        scattered.put(address(1,0,120),new ItemStack(1,1,0,new byte[0]));
        scattered.put(address(1,0,130),new ItemStack(1,1,1,new byte[0]));
        scattered.put(address(1,0,50),new ItemStack(2,7,0,new byte[0]));
        scattered.put(address(1,0,70),new ItemStack(2,3,0,new byte[0]));
        scattered.put(address(1,0,100),new ItemStack(4,1,0,new byte[0]));
        scattered.put(address(1,0,140),new ItemStack(3,1,0,new byte[0]));
        scattered.put(address(1,0,150),new ItemStack(6,1,0,new byte[0]));
        scattered.put(address(0,6,0),new ItemStack(1,1,0,new byte[0]));
        Inventory before=new Inventory(catalog).replace(scattered,0).after();
        Inventory.Plan plan=before.sort(address(1,0,0));Inventory after=plan.after();
        check(count(after,1,0,2)==10,"identical stacks merge while sorting");
        check(count(after,1,0,1)==2&&count(after,1,0,3)==1&&count(after,1,0,4)==1,"sorting conserves every item count");
        check(after.at(address(0,6,0)).json().equals(before.at(address(0,6,0)).json()),"equipment never moves during bag sort");
        for(int slot:new int[]{50,70,100,120,130,140,150})check(after.at(address(1,0,slot))==null,"old bag cell cleared "+slot);
        List<Integer> order=after.items().entrySet().stream().filter(e->e.getKey().aggregate()==1).sorted(Map.Entry.comparingByKey()).map(e->e.getValue().id()).toList();
        check(order.equals(List.of(1,1,6,4,2,3)),"items pack as weapons, containers, armor, ammo, then medicine");
        check(plan.changes().size()>0&&after.revision()==before.revision()+1,"sort is one inventory revision");
        Inventory.Plan stable=after.sort(address(1,0,0));
        check(stable.changes().isEmpty(),"repeated sort is idempotent");
        Map<Inventory.Address,ItemStack> storage=new LinkedHashMap<>();
        storage.put(address(2,0,300),new ItemStack(3,1,0,new byte[0]));storage.put(address(2,0,20),new ItemStack(2,4,0,new byte[0]));storage.put(address(2,1,10),new ItemStack(2,5,0,new byte[0]));
        Inventory withStorage=after.replace(storage,0).after();Inventory sortedStorage=withStorage.sort(address(2,0,0)).after();
        check(sortedStorage.at(address(2,0,0)).id()==2&&sortedStorage.at(address(2,0,1)).id()==3,"storage page compacts independently");
        check(sortedStorage.at(address(2,1,10)).id()==2,"other storage pages are untouched");
        rejected(()->before.sort(address(0,0,0)));
        rejected(()->before.sort(address(1,0,1)));
        rejected(()->withStorage.sort(address(2,12,0)));
        check(InventoryPackets.sort(sortBody(0,1,0,0),4,false).equals(address(1,0,0)),"native sort request reads the bag root");
        check(InventoryPackets.sort(sortBody(0,2,3,0),4,true).equals(address(2,3,0)),"native sort request reads an explicit storage page");
        check(InventoryPackets.sort(sortBody(0,0,0,0),4,true).equals(address(2,4,0)),"storage local root resolves to the open page");
        check(InventoryPackets.sort(sortBody(0,0,7,0),4,true).equals(address(2,7,0)),"storage local page carried in the container field");
        check(InventoryPackets.sort(sortBody(5,0,0,0),4,true).equals(address(2,5,0)),"storage identity overrides the open page");
        check(InventoryPackets.sort(sortBody(0,0,255,0),4,true).equals(address(2,4,0)),"an unset local container falls back to the open page");
        check(InventoryPackets.sort(sortBody(0,1,0,0),4,true).equals(address(1,0,0)),"bag sorts while storage is also open");
        rejected(()->{try{InventoryPackets.sort(sortBody(0,1,0,5),4,false);}catch(IOException error){throw new IllegalArgumentException(error.getMessage());}});
        rejected(()->{try{InventoryPackets.sort(sortBody(0,3,0,0),4,false);}catch(IOException error){throw new IllegalArgumentException(error.getMessage());}});
        rejected(()->{try{InventoryPackets.sort(sortBody(99,0,0,0),4,true);}catch(IOException error){throw new IllegalArgumentException(error.getMessage());}});
        rejected(()->{try{InventoryPackets.sort(sortBody(0,0,0,0),4,false);}catch(IOException error){throw new IllegalArgumentException(error.getMessage());}});
        System.out.println("Inventory sort: "+checks+" PASS");
    }
}