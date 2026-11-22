package szserver.net;

import szserver.inventory.*;
import java.io.*;
import java.util.*;
import com.github.luben.zstd.Zstd;

/** Current native inventory wire formats, kept outside the inventory domain. */
public final class InventoryPackets {
    public static final int SORT_ID=1506;
    public record Request(int owner,int sequence,Inventory.Address source,List<Inventory.Address> destinations,int count,boolean split,int targetAggregate) {}
    private static Inventory.Address address(DataInputStream in,int owner,int page)throws IOException {
        int aggregate=in.readUnsignedByte(),container=in.readUnsignedByte(),slot=in.readUnsignedShort();
        if(owner==-1&&aggregate==0&&container==0)return new Inventory.Address(2,page,slot);
        return new Inventory.Address(aggregate,container,slot);
    }
    public static Request read(int id,byte[] body,int page)throws IOException {
        DataInputStream in=Wire.input(body);int owner,sequence,count=0,target=-1;boolean split=id==102||id==2040;Inventory.Address source;List<Inventory.Address> destinations=new ArrayList<>();
        if(id==1231||id==102||id==2009){
            if(id==102)count=in.readInt();owner=in.readShort();sequence=in.readShort();source=address(in,owner,page);
            if(id==2009){target=in.readUnsignedByte();int n=in.readUnsignedShort();if(n==0||n>StorageLayout.MAX_CAPACITY)throw new IOException("quick_destination_limit");for(int i=0;i<n;i++)destinations.add(address(in,owner,page));count=in.readInt();}
            else destinations.add(address(in,owner,page));
        }else if(id==1627||id==2040){
            // Owner trails the nullable addresses in these native variants.
            int ownerOffset=body.length-4;if(ownerOffset<5)throw new IOException("inventory_request_size");owner=java.nio.ByteBuffer.wrap(body).getShort(ownerOffset);
            source=address(in,owner,page);int nil=in.readUnsignedByte();if(nil>1)throw new IOException("inventory_null_marker");if(nil==0)destinations.add(address(in,owner,page));
            if(split){count=in.readInt();in.readUnsignedByte();}if(in.readShort()!=owner)throw new IOException("inventory_owner");sequence=in.readShort();
        }else throw new IOException("inventory_request_kind");
        if(in.available()!=0||count<0||split&&count==0||owner!=0&&owner!=-1)throw new IOException("inventory_request_layout");
        return new Request(owner,sequence,source,List.copyOf(destinations),count,split,target);
    }
    /** Native sort request: the container identity, its unused sequence default, then the container root address.
     * Storage screens encode their local root as aggregate 0 with container 0 or the page, so the open page resolves that form. */
    public static Inventory.Address sort(byte[] body,int page,boolean storageOpen)throws IOException {
        DataInputStream in=Wire.input(body);int identity=in.readShort();in.readShort();Inventory.Address raw=new Inventory.Address(in.readUnsignedByte(),in.readUnsignedByte(),in.readUnsignedShort());
        Inventory.Address target;
        // The storage screen keeps its page in a local aggregate-0 model; its grid root is
        // either (0,0) or (0,page). Only resolve that form while a storage page is open.
        if(raw.aggregate()==0){
            if(!storageOpen)throw new IOException("inventory_sort_request");
            int targetPage=identity>0?identity:raw.container()>0&&raw.container()<StorageLayout.PAGES?raw.container():page;
            if(targetPage<0||targetPage>=StorageLayout.PAGES)throw new IOException("inventory_sort_request");
            target=new Inventory.Address(2,targetPage,raw.slot());
        }else target=raw;
        if(in.available()!=0||target.slot()!=0||target.aggregate()!=1&&target.aggregate()!=2||target.aggregate()==1&&target.container()!=0||!StorageLayout.validAddress(target))throw new IOException("inventory_sort_request");
        return target;
    }
    private static void address(DataOutputStream out,Inventory.Address a,int owner)throws IOException {out.writeByte(a.aggregate()==2&&owner==-1?0:a.aggregate());out.writeByte(a.aggregate()==2&&owner==-1?0:a.container());out.writeShort(a.slot());}
    public static byte[] stack(ItemStack stack){return Wire.bytes(out->{out.writeBoolean(stack!=null);if(stack!=null){out.writeShort(stack.id());out.writeInt(stack.count());out.writeShort(stack.metadata());byte[] state=stack.state();out.writeBoolean(state.length>0);out.write(state);out.write(stack.properties());}});}
    public static byte[] snapshot(Inventory inventory,int owner,int aggregate,int page){
        var entries=inventory.items().entrySet().stream().filter(e->aggregate==0&&owner==-1?e.getKey().aggregate()==2&&e.getKey().container()==page:e.getKey().aggregate()==aggregate).toList();
        byte[] raw=Wire.bytes(out->{out.writeByte(aggregate);out.writeShort(entries.size());for(var e:entries)address(out,e.getKey(),owner);for(var e:entries)out.write(stack(e.getValue()));});byte[] compressed=Zstd.compress(raw,3);
        return Wire.dynamic("998343761",Wire.bytes(out->{out.writeShort(owner);out.writeShort(-1);out.writeInt(compressed.length);out.write(compressed);}));
    }
    public static byte[] point(int owner,int sequence,Inventory.Address address,ItemStack stack){return Wire.dynamic("1090368269",Wire.bytes(out->{out.writeShort(owner);out.writeShort(sequence);out.writeBoolean(stack!=null);if(stack!=null)out.write(stack(stack));address(out,address,owner);}));}
    public static byte[] ack(int owner,int sequence){return Wire.dynamic("1255589210",Wire.bytes(out->{out.writeShort(owner);out.writeShort(sequence);}));}
    public static byte[] open(){return Wire.dynamic("1603754623",Wire.bytes(out->out.writeInt(0)));}
    public static byte[] storage(int page){return Wire.dynamic("920939658",Wire.bytes(out->{out.writeInt(page);out.writeInt((1<<15)-1);out.writeInt(-1);}));}
}
