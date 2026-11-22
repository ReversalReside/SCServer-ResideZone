package szserver.net;

import szserver.client.*;
import environment.protocol.EnvironmentWire;
import scvehicles.protocol.VehicleWire;
import scvehicles.protocol.VehicleAction;
import scvehicles.protocol.VehicleActionProfiles;
import scvehicles.protocol.VehicleSeats;
import szserver.commands.CommandService;
import szserver.commands.WorldEditChat;
import szserver.util.*;
import szserver.inventory.*;
import szserver.persistence.CharacterLocations;
import szserver.world.World;
import szserver.world.WorldLoop;
import java.io.*;
import java.net.*;
import java.nio.*;
import java.nio.file.*;
import java.security.SecureRandom;
import java.util.*;
import java.util.concurrent.*;
import com.google.gson.JsonObject;

/** Loopback multiplayer arena. One owner for transport/session/combat state.
 * Fixed tick deadlines; bounded reliable windows; native serializers warmed before listening.
 * Durable account/ticket IO runs on bounded login and storage workers.
 */
public final class GameServer implements AutoCloseable {
    private static final int MAX_SESSIONS=32,WINDOW=128,MAX_QUEUE=2048,FRAGMENT=1100;
    private record LauncherReload(long deadline,long life,int hand,UUID firearm,UUID launcher,int ammo,ItemStack expected) {}
    private record ShotKey(long seed,int sequence){}
    private record Swing(int kind,int combo,int weapon,long ready,long expires,MeleeProfile profile){}
    private record BotKey(String id,long incarnation){}
    private record Recipient(InetSocketAddress address,UUID incarnation){}
    private final NativeEntityIds entityIds=new NativeEntityIds();
    private final szserver.world.WorldItems worldItems;private final String itemWorld;private long itemCollected,itemDropped,itemFailures,itemRequests;private String lastItemFailure="";private final Set<UUID> remoteItemExpiry=new HashSet<>();
    private final CorpseLifecycle<Recipient> corpses=new CorpseLifecycle<>(entityIds);
    private final PlayerClones<Session> clones=new PlayerClones<>(entityIds);
    private long poseRevision,completionRequests,cloneHits,cloneKills;
    private long respawnCooldowns,respawnRetries;
    private static BotKey botKey(World.View view){return new BotKey(view.id(),view.incarnation());}
    private record Sent(byte[] payload,long last,int attempts){}
    private static final class Teleport {World.Vec target;boolean surface,respawnRequested;int phase;long deadline,nextRetry;CompletableFuture<Integer> height;CompletableFuture<Void> save;TerrainProjection.Prepared center;String reply;Teleport(World.Vec target,boolean surface,String reply,long now){this.target=target;this.surface=surface;this.reply=reply;deadline=now+8_000_000_000L;}}
    private record ParseTarget(Session session,int lane){}
    private static final class Lane {boolean parsing;int rx,tx;final Map<Integer,byte[]> received=new HashMap<>();final LinkedHashMap<Integer,Sent> sent=new LinkedHashMap<>();final ArrayDeque<byte[]> queued=new ArrayDeque<>();final ReliableInput stream=new ReliableInput();}
    private record ScaleChange(Session requester,double[] position,playerscale.ScaleValue scale,CompletableFuture<Integer> query,long deadline){}
    private long scaleRevision;private String lastScaleError="";
    private static final class Session {
        float characterScale=1,configuredScale=1;boolean scaleCapable,scaleSaving,scaleRestore;long scaleHelloAt;ScaleChange scaleChange;
        final PlayerAnimation animation=new PlayerAnimation();final PlayerReplication replication=new PlayerReplication();final PlayerReplication botReplication=new PlayerReplication();final PlayerMotion motion=new PlayerMotion();String forcedAnimation;long lastCompletion;final Map<Integer,String> poses=new HashMap<>();
        boolean vehicleCapable,vehicleArticulation,vehicleFleet,vehicleContacts,vehicleInteractions;long vehicleCommandAt;
        boolean fpvCapable;long fpvOwnerSent;final Map<UUID,Long> fpvWorldSent=new HashMap<>();final Set<UUID> fpvUsed=new HashSet<>();
        final InetSocketAddress address;final UUID incarnation=UUID.randomUUID();final int id,clientId;final byte[] cookie;final Lane[] lanes={new Lane(),new Lane()};
        SicknessState sickness=SicknessState.EMPTY;Map<String,SicknessRules.State> sicknessSent=Map.of();Map<String,Float> sicknessExposure=Map.of(),sicknessExposureSent=Map.of();long sicknessSendAt;CharacterEffects effects=new CharacterEffects();GrenadePulseEffects pulses=new GrenadePulseEffects();final AreaEffects areaEffects=new AreaEffects();List<AreaExposure.Contact> selectedAreas=List.of();Map<String,Float> stats=Map.of();long lastDamage,lastVitals,damageWall,deadUntil,effectsExpireBefore;double[] velocity=new double[3];CharacterLocations.PlayerState restoredState,lastStateRequest;CharacterLocations.StateSave stateSave,respawnStateSave;boolean vitalsQueued;
        byte[] movementConfiguration;float stamina=1,maxStamina=1,observedStamina=1;long lastInput,lastStaminaCorrection;Float reportedStamina;
        final Set<BotKey> bots=new HashSet<>();final Map<BotKey,Float> botHealth=new HashMap<>(),botMaximum=new HashMap<>();final Map<BotKey,szserver.world.ActorPresentation> botAppearances=new HashMap<>();
        TerrainProjection.Prepared completeTiles;int completeTileAt;
        final ChunkFaults faults=new ChunkFaults();final ChunkDeliveryBook deliveries=new ChunkDeliveryBook();final Map<Long,UUID> cachedRequests=new HashMap<>();long manifestRetryAt;
        final ChunkWindow window;CompletableFuture<List<ChunkManifests.Row>> manifest;List<Long> manifestKeys=List.of();long windowEpoch,manifestEpoch;
        final WorldEditChat editor=new WorldEditChat();
        PlayerRules.Mode gameplayMode=PlayerRules.Mode.STALKER;String faction="stalker";
        final GrenadeUse grenadeUse=new GrenadeUse();int grenadePrediction=-1;
        UUID grenadeDeath;long reviveCheckAt;CorpseLifecycle.Corpse<Recipient> lastCorpse;Swing swing;long nextAttack,life,deathGeneration,drawUntil;Integer lastShotClientTick;
        CompletableFuture<LocalAccounts.Grant> authentication;UUID loginProtocol;CompletableFuture<Integer> placement;CompletableFuture<Void> spawnSave;CompletableFuture<Integer> spawnCheck;long spawnDeadline,nextSpawnCheck;World.Vec requestedSpawn;UUID spawnScope;double[][] candidates;int candidate;long placementDeadline,nextPlacement;boolean positioned,inventoryReady,respawning;long last=System.nanoTime();boolean accepted,ready;final InventoryScreen inventoryScreen=new InventoryScreen();int handContainer=6;GameInventory.Weapon weapon;LocalAccounts.Grant grant;int entity,clientTick,sequence,rounds=30,reserve=90;long lastShot,reloadAt;Unjamming unjamming;boolean unjammingPending;float hp=100,maxHp=100,yaw,pitch;double[] pos;byte[][] magazines;final PlayerPosture posture=new PlayerPosture();int mode;double speed;boolean ground=true,holstered;UUID firearm;final ChunkSubscriptions chunks=new ChunkSubscriptions();final Map<Long,Long> chunkRevisions=new HashMap<>();TerrainProjection.Prepared deltas;long deltaChunk;int deltaAt;final Set<Long> pendingChunks=new LinkedHashSet<>();int chunkX=Integer.MIN_VALUE,chunkZ=Integer.MIN_VALUE;final Map<ShotKey,Long> seenShots=new LinkedHashMap<>();
        Session(InetSocketAddress address,int id,int clientId,byte[] cookie,ChunkStreamingSettings settings){this.address=address;this.id=id;this.clientId=clientId;this.cookie=cookie;window=new ChunkWindow(settings.distance(),settings.outstandingLimit());}
        long authenticationOrder;CompletableFuture<Boolean> accountPreparation;CompletableFuture<Void> inventoryPreparation;boolean inventoryPinned;CharacterLocations.Lease locationLease;UUID locationProtocol;Teleport teleport;final TeleportGuard teleportGuard=new TeleportGuard();CompletableFuture<Void> warpSave;String warpReply;CompletableFuture<World.Vec> warpRead;String warpReadName;long warpReadLife;
        final PortalLookup portalLookup=new PortalLookup();long portalUntil;
        CompletableFuture<szserver.persistence.WorldEnvironment.State> environmentSave;boolean weatherCommand;final EnvironmentAccess.Client environmentClient=new EnvironmentAccess.Client();
        final ArrayDeque<CompletableFuture<NativeBlockAccess.Outcome>> blockSaves=new ArrayDeque<>();final LinkedHashMap<Long,BlockRepair> blockRepairs=new LinkedHashMap<>();
        UUID anomalyStasis;long anomalyMirrorUntil;float anomalySlow=1;final Set<UUID> anomalyCircuses=new HashSet<>();
        final Map<Integer,CompletableFuture<byte[]>> sceneReads=new LinkedHashMap<>();final UUID sceneWriter=UUID.randomUUID();final ArrayDeque<CompletableFuture<JsonObject>> sceneSaves=new ArrayDeque<>();
        PhysicalReload.Cycle physicalCycle;boolean physicalPending;PhysicalReload.Request physicalLast;UUID physicalIdentity;byte[] physicalReply;
        final WeaponActions weaponActions=new WeaponActions();LauncherReload launcherReload;
        Inventory.Address adminSelected;int creativeSelected;final Map<Integer,Integer> creativeHints=new HashMap<>();
        final ArrayDeque<NativeGrant> nativeGrants=new ArrayDeque<>();int nativeGrantBytes;boolean nativeGrantPending;
        final Set<UUID> worldItemsSeen=new HashSet<>();int dropSequence=-1;long lastItemDrop;
        int lootboxItem=-1,lootSequence;final LinkedHashMap<Integer,LootReply> lootReplies=new LinkedHashMap<>();
    }
    private static final class BlockRepair {final Set<World.Block> points=new LinkedHashSet<>();CompletableFuture<NativeBlockAccess.Repair> future;}
    private long physicalAccepted,physicalDenied,physicalDuplicates;
    private long replicationFrames,replicationRecords,replicationBytes,replicationDeferred;
    private record NativeGrant(Session target,NativeAdminPackets.Give give,Inventory.Address weapon,ItemStack expected,long life,long deadline,int bytes){}
    private static final class LootReply {final LootboxPackets.Open request;byte[] frame;LootReply(LootboxPackets.Open request){this.request=request;}}
    private long nativeAdminAccepted,nativeAdminDenied,nativeAdminEchoes;
    private final NativeBlockAccess nativeBlocks;private long nativeBlockDenied,nativeBlockFailed,nativeBlockIntents,nativeBlockRepairs;
    private final CommandService commands;
    private final ZombieHorde zombies;
    private final szserver.commands.SceneAccess sceneAccess;private long sceneDenied,sceneFailed;
    private final AnomalySimulation anomalies;
    private CompletableFuture<szserver.world.MapObstructionAccess.AnomalyQuery> anomalyQuery;
    private szserver.world.MapObstructionAccess.AnomalyQuery anomalyScene;
    private long anomalyClock,anomalySceneAt,anomalyErrors,anomalyVelocityEchoes;
    private final Set<UUID> worldEditors;
    private final PlayerRules playerRules;
    private final LocalAccounts accounts;
    private final CharacterLocations locations;
    private final szserver.persistence.WarpStore warps;
    private final szserver.persistence.WorldEnvironment environment;private final EnvironmentAccess environmentAccess;private long environmentRevision=-1,nextEnvironmentClock;
    private long teleports,teleportErrors,staleTeleportInputs;
    private long portalQueries,portalActivations,portalErrors;
    private long nextLocationFlush;
    private final GameInventory inventories;
    private static final class VitalsTask {final UUID id;final Session session;final Runnable finished;CompletableFuture<Void> save;boolean held=true;VitalsTask(UUID id,Session session,Runnable finished){this.id=id;this.session=session;this.finished=finished;}}
    private record ReviveCheck(Session target,GrenadeAreaStore.Activation source,int profile,UUID death,long life,double[] position,CompletableFuture<Integer> placement){}
    private final Map<UUID,ReviveCheck> reviveChecks=new LinkedHashMap<>();private long revives,reviveDenied,reviveErrors;
    private final Map<UUID,VitalsTask> medicineCommits=new LinkedHashMap<>(),deathCommits=new LinkedHashMap<>(),areaHealCommits=new LinkedHashMap<>(),pulseCommits=new LinkedHashMap<>(),reviveCommits=new LinkedHashMap<>();
    private long areaHealApplied,areaHealDenied;
    private long vitalsCommits,vitalsCommitFailures;
    private final AdminInventories adminInventories;
    private final ArrayBlockingQueue<Runnable> adminTasks=new ArrayBlockingQueue<>(64);
    private final ClientScreenshots screenshots=new ClientScreenshots();
    public ClientScreenshots screenshots(){return screenshots;}
    public CompletableFuture<Map<String,Object>> requestScreenshots(UUID request){
        var result=new CompletableFuture<Map<String,Object>>();
        try{adminTask(()->{try{
            var clients=sessions.values().stream().filter(s->s.ready).map(s->new ClientScreenshots.Client(s.incarnation,character(s),s.grant.name())).toList();
            result.complete(screenshots.begin(request,clients,client->{
                Session target=sessions.values().stream().filter(s->s.ready&&s.incarnation.equals(client.session())).findFirst().orElseThrow();
                enqueue(target,0,ScreenshotPackets.request());
            }));
        }catch(RuntimeException error){result.completeExceptionally(error);}});}catch(RuntimeException error){result.completeExceptionally(error);}
        return result;
    }
    private final Map<Integer,GameItems.Medicine> medicines;
    private final Map<String,Integer> statOrdinals;
    private final HealthRules healthRules;private final SicknessRules sicknessRules;private long sicknessTicks,sicknessHits,sicknessUpdates;
    private final StaminaRules staminaRules;
    private final WorldLoop world;
    private final Vehicles<Session> vehicles=new Vehicles<>();
    private final VehicleContacts<Session> vehicleContacts=new VehicleContacts<>();
    private final VehicleActions<Session> vehicleActions=new VehicleActions<>();
    private Map<BotKey,VehicleImpacts.Pending> vehicleBotImpacts;
    private int vehicleContactSequence;
    private final FpvDrones<Session> drones=new FpvDrones<>();
    private final Map<UUID,Set<Session>> droneObservers=new HashMap<>();
    private FpvExplosion fpvExplosion;
    private GameGrenades grenadeBridge;private final GrenadeOutbox grenadeOutbox;private final GrenadeAreaStore grenadeAreas;private final GrenadeCompletion grenadeCompletion;private final GrenadeAreaScenes grenadeAreaScenes;
    private final Map<Integer,GrenadePackets.Profile> grenadeProfiles=new HashMap<>();private final Map<Integer,GrenadeUse.Rules> grenadeUseRules=new HashMap<>();
    private final AreaRelations areaRelations;private final int continuousEffectKind,instantEffectKind;private final Map<Integer,BlastRules> blastRules=new HashMap<>();
    private final Map<Integer,List<GrenadeAreaProfile>> grenadeAreaProfiles=new HashMap<>(),enabledAreaProfiles=new HashMap<>();
    private static final class AreaScene {final GrenadeAreaStore.Activation activation;final int handle,index;CompletableFuture<GrenadeAreaScenes.Scene> prepared;GrenadeAreaScenes.Scene scene;float fraction=Float.NaN;final Map<Recipient,Integer> recipients=new HashMap<>();AreaScene(GrenadeAreaStore.Activation activation,int index,int handle){this.activation=activation;this.index=index;this.handle=handle;}}
    private record ShieldShot(Session shooter,AcceptedShot shot,GrenadeAreaScenes.ShieldContact contact,float damage,long admitted,UUID request,CompletableFuture<GrenadeAreaStore.ShieldHit> future){}
    private final ArrayDeque<ShieldShot> shieldShots=new ArrayDeque<>();private long shieldBlocked,shieldErrors;
    private CombatBarrierQueue combatBarriers;private final HordeCombat hordeCombat=new HordeCombat();
    private final Map<UUID,AreaScene> areaScenes=new LinkedHashMap<>();private int nextAreaHandle=Integer.MIN_VALUE;private long areaSceneAt,areaSceneSpawns,areaSceneRemovals;
    private final Map<Integer,String> grenadeUnavailable=new TreeMap<>();
    private static final class ThrownGrenade {final Session owner;final UUID character;final String name;final AreaRelations.Actor context;final int prediction;final GrenadePackets.Profile profile;int entity;final Set<Recipient> recipients=new HashSet<>();ThrownGrenade(Session owner,int prediction,GrenadePackets.Profile profile){this(owner,owner.grant.characterId(),owner.grant.name(),null,prediction,profile);}ThrownGrenade(Session owner,UUID character,String name,AreaRelations.Actor context,int prediction,GrenadePackets.Profile profile){this.owner=owner;this.character=character;this.name=name;this.context=context;this.prediction=prediction;this.profile=profile;}}
    private final Map<UUID,ThrownGrenade> thrownGrenades=new HashMap<>();
    /** Retain the last living native pose; a controller may reset posture when HP reaches zero. */
    private final Map<BotKey,World.View> livingBotPoses=new HashMap<>();
    private final Set<BotKey> emittedBotCorpses=new HashSet<>();
    private record GrenadeTarget(Session player,long life,PlayerClones.Clone<Session> cloneTarget,World.View bot,double[] position,Map<String,Float> stats,double height){
        GrenadeTarget(Session player,long life,PlayerClones.Clone<Session> cloneTarget,World.View bot,double[] position,Map<String,Float> stats){this(player,life,cloneTarget,bot,position,stats,1.8);}
    }
    private record GrenadeBlast(szserver.world.Grenades.Explosion event,List<GrenadeTarget> visible){}
    private record GrenadeBatch(List<szserver.world.Grenades.View> views,List<GrenadeBlast> blasts,List<szserver.world.Grenades.Saved> saved,szserver.world.MapObstructionAccess.Authorization geometry){
        GrenadeBatch(List<szserver.world.Grenades.View> views,List<GrenadeBlast> blasts,List<szserver.world.Grenades.Saved> saved){this(views,blasts,saved,null);}
    }
    private Map<UUID,GrenadeAreaStore.Activation> pendingPulseSources=Map.of();private long pulseApplied,pulseDenied;
    private CompletableFuture<List<GrenadeAreaStore.Activation>> grenadeAreaExpiry;private long grenadeAreaExpireAt;
    private CompletableFuture<GrenadeBatch> grenadePoll;private CompletableFuture<Void> grenadeSave;private CompletableFuture<Boolean> grenadeAck;private GrenadeBatch grenadeDelivery;private long grenadeRecovered;
    private CompletableFuture<List<szserver.world.ActorMineTrigger>> minePoll;
    private long mineExplosions,mineHits,minePollErrors;
    private void pollMines(){
        if(minePoll==null)minePoll=world.submit(w->{var events=w.mineExplosions().stream().limit(8).toList();for(var event:events)w.acknowledgeMine(event.id());return events;});
    }
    private void advanceMines(){
        if(minePoll==null||!minePoll.isDone())return;var future=minePoll;minePoll=null;
        List<szserver.world.ActorMineTrigger> events;try{events=future.join();}catch(CompletionException unavailable){minePollErrors++;return;}
        Map<UUID,Session> current=new HashMap<>();for(var session:sessions.values())if(session.ready)current.put(character(session),session);
        for(var event:events){
            mineExplosions++;double[] origin={event.position().x(),event.position().y(),event.position().z()};
            for(var hit:event.players()){
                Session victim=current.get(hit.character());
                if(victim!=null&&victim.incarnation.equals(hit.incarnation())&&victim.life==hit.life()&&victim.hp>0&&!victim.respawning&&victim.teleport==null&&victim.gameplayMode==PlayerRules.Mode.STALKER){damage(null,"Мина",victim,hit.damage(),origin,null,new double[3],false);mineHits++;}
            }
            byte[] fx=Wire.dynamic("1155277618",event.profile().presentation(origin[0],origin[1],origin[2]));
            for(Session viewer:List.copyOf(sessions.values()))if(viewer.ready){double x=viewer.pos[0]-origin[0],z=viewer.pos[2]-origin[2];if(x*x+z*z<=256*256)enqueueOrRetire(viewer,fx);}
        }
    }
    private record Unjamming(long deadline,long life,int hand,UUID identity,ItemStack expected){}
    private long unjammingStarted,unjammingCompleted,unjammingCanceled,unjammingDenied;
    private long primaryLauncherShots,primaryLauncherDenied,underbarrelShots,underbarrelDenied,underbarrelReloads,underbarrelReloadCanceled;
    private long grenadeThrows,grenadeDenied,grenadeExplosions,grenadeHits,grenadePollErrors,grenadeHeldExplosions,grenadeUseCanceled,grenadePrepareDenied;
    private long fpvDenied,fpvExplosions;
    private final szserver.world.MapObstructionAccess fpvMap;private final FpvReceipts fpvReceipts;
    private final Map<UUID,FpvPlacementTask> fpvPlacements=new HashMap<>(),fpvPlacementIds=new HashMap<>();private long fpvPlaced;
    private static final class FpvPlacementTask {
        final Session owner;final UUID character;final FpvPackets.Request request;final long life,deadline;double[] operator;
        CompletableFuture<szserver.world.MapObstructionAccess.Authorization> geometry;final CompletableFuture<FpvReceipts.Admission> admission;
        boolean debitStarted;CompletableFuture<Void> receipt;String error;
        FpvPlacementTask(Session owner,UUID character,FpvPackets.Request request,CompletableFuture<szserver.world.MapObstructionAccess.Authorization> geometry,CompletableFuture<FpvReceipts.Admission> admission){this.owner=owner;this.character=character;this.request=request;this.geometry=geometry;this.admission=admission;life=owner.life;operator=owner.pos.clone();deadline=System.nanoTime()+8_000_000_000L;}
    }
    private final szserver.world.RegionTerrain regions;
    private final double[] spawn;
    private final Map<BotKey,Integer> botEntities=new HashMap<>();
    private final Map<BotKey,Float> botHeadings=new HashMap<>();
    private final ArrayBlockingQueue<Runnable> combatCompleted=new ArrayBlockingQueue<>(256);
    private final java.util.concurrent.atomic.AtomicLong combatCompletionDrops=new java.util.concurrent.atomic.AtomicLong();
    private long botHits,botDeaths,botDamageErrors,blockedHits,visibilityErrors;
    private final Map<Integer,byte[]> appearances=new HashMap<>();
    private final ServerSocket gateway;
    private final DatagramSocket udp;
    private final Thread transport,acceptor;
    private final ThreadPoolExecutor logins=Workers.pool("gateway-login",4,32);
    private final CharacterSessions<Session> characterSessions=new CharacterSessions<>();
    private long authenticationOrder;
    private final ClientPackets nativePackets;
    private final GamePackets packets;
    private final TerrainProjection terrainProjection;
    private final ChunkStreamingSettings chunkSettings;
    private final ChunkManifests chunkManifests;private long manifestsSent,manifestEntries,manifestStale,manifestFailures,chunkCacheHits,chunkFullResponses,chunkDuplicateDemands,chunkDeferredDemands,chunkReleased;
    private final Map<InetSocketAddress,Session> sessions=new LinkedHashMap<>();
    private final byte[][] baseMagazines=new byte[31][];
    private final ConnectionRoutes routes;
    private volatile boolean running,closed;
    private volatile Map<String,Object> metrics=Map.of();
    private int tick;
    private long rejected,inputs,shots,hits,retries,retired,maxTickNanos,skipped,deaths,respawns,tileRefreshes,bulkBlockRefreshes;
    private long ownerStarted,lastTickBegin,maxTickGapNanos,maxTickLateNanos,maxReceiveNanos,maxCompletionNanos,maxSocketWaitNanos;
    private final ArrayDeque<Map<String,Object>> lateTicks=new ArrayDeque<>();
    private final ArrayDeque<ParseTarget> parseReady=new ArrayDeque<>();
    private final ArrayDeque<Map<String,Object>> slowPackets=new ArrayDeque<>();
    private long maxApplicationNanos,applicationBudgetYields;
    private Map<String,Object> lastShot=Map.of();
    private Map<String,Object> lastReload=Map.of();private final Map<Integer,Long> applicationCounts=new HashMap<>();
    private Map<String,Object> lastMelee=Map.of();
    private final Map<Integer,Long> unknown=new HashMap<>();private final Map<Integer,Long> datagrams=new HashMap<>();private long connectionMismatch;
    private final CharacterDeletions characterDeletions;
    private final SecureRandom random=new SecureRandom();
    private long maxHelloCookieNanos,maxHelloSessionNanos,maxHelloChallengeNanos;
    public GameServer(Path root,Path data,Path runtime,Path registry,int httpPort,int gatewayPort,int udpPort,WorldLoop world,szserver.world.Terrain terrain,double[] spawn,CommandService commands,Set<UUID> worldEditors)throws Exception{
        this(root,data,runtime,registry,httpPort,gatewayPort,udpPort,world,terrain,spawn,commands,worldEditors,PlayerRules.development());
    }
    public GameServer(Path root,Path data,Path runtime,Path registry,int httpPort,int gatewayPort,int udpPort,WorldLoop world,szserver.world.Terrain terrain,double[] spawn,CommandService commands,Set<UUID> worldEditors,PlayerRules playerRules)throws Exception{
        this(root,data,runtime,registry,httpPort,gatewayPort,udpPort,world,terrain,spawn,commands,worldEditors,playerRules,ConnectionRoutes.loopback(gatewayPort,udpPort));
    }
    public GameServer(Path root,Path data,Path runtime,Path registry,int httpPort,int gatewayPort,int udpPort,WorldLoop world,szserver.world.Terrain terrain,double[] spawn,CommandService commands,Set<UUID> worldEditors,PlayerRules playerRules,ConnectionRoutes routes)throws Exception{
        this(root,data,runtime,registry,httpPort,gatewayPort,udpPort,world,terrain,spawn,commands,worldEditors,playerRules,routes,ChunkStreamingSettings.defaults());
    }
    public GameServer(Path root,Path data,Path runtime,Path registry,int httpPort,int gatewayPort,int udpPort,WorldLoop world,szserver.world.Terrain terrain,double[] spawn,CommandService commands,Set<UUID> worldEditors,PlayerRules playerRules,ConnectionRoutes routes,ChunkStreamingSettings chunkSettings)throws Exception{
        this(root,data,runtime,registry,httpPort,gatewayPort,udpPort,world,terrain,spawn,commands,worldEditors,playerRules,routes,chunkSettings,FpvPlacementSettings.defaults(registry));
    }
    public GameServer(Path root,Path data,Path runtime,Path registry,int httpPort,int gatewayPort,int udpPort,WorldLoop world,szserver.world.Terrain terrain,double[] spawn,CommandService commands,Set<UUID> worldEditors,PlayerRules playerRules,ConnectionRoutes routes,ChunkStreamingSettings chunkSettings,FpvPlacementSettings fpvSettings)throws Exception{
        this(root,data,runtime,registry,httpPort,gatewayPort,udpPort,world,terrain,spawn,commands,worldEditors,playerRules,routes,chunkSettings,fpvSettings,true);
    }
    /** Prepare native codecs, inventories, stores and bound sockets without starting gameplay. */
    public GameServer(Path root,Path data,Path runtime,Path registry,int httpPort,int gatewayPort,int udpPort,WorldLoop world,szserver.world.Terrain terrain,double[] spawn,CommandService commands,Set<UUID> worldEditors,PlayerRules playerRules,ConnectionRoutes routes,ChunkStreamingSettings chunkSettings,FpvPlacementSettings fpvSettings,boolean autoStart)throws Exception{
        szserver.client.NpcVisuals.prepare(runtime);
        NpcEditorHooks.prepare(this,data,runtime);
        ArrayDeque<AutoCloseable> rollback=new ArrayDeque<>();List<Thread> started=new ArrayList<>();rollback.push(screenshots);
        try{
        rollback.push(()->Workers.stop(logins));
        // SecureRandom lazily seeds on first use; keep that cold path off the game owner.
        random.nextBytes(new byte[20]);UUID.randomUUID();
        this.routes=Objects.requireNonNull(routes);this.chunkSettings=Objects.requireNonNull(chunkSettings);
        // Prepare the same session/challenge call sites before the transport owner receives its first hello.
        challengeBytes(new Session(new InetSocketAddress("127.0.0.1",0),0,0,new byte[20],this.chunkSettings));
        this.playerRules=Objects.requireNonNull(playerRules);this.fpvUseSettings=Objects.requireNonNull(fpvSettings);
        this.commands=Objects.requireNonNull(commands);this.worldEditors=Set.copyOf(worldEditors);
        sceneAccess=new szserver.commands.SceneAccess(world,commands);rollback.push(sceneAccess);
        healthRules=HealthRules.read(com.google.gson.JsonParser.parseString(Files.readString(root.resolve("fixtures/artifact-settings.json"))).getAsJsonObject());
        this.world=Objects.requireNonNull(world);this.regions=terrain instanceof szserver.world.RegionTerrain r?r:null;this.spawn=spawn.clone();
        var anomalyResources=world.actorMotion() instanceof NativeActorMotion nativeMotion&&nativeMotion.anomalyResources()!=null?nativeMotion.anomalyResources():fpvSettings.boxes();anomalies=new AnomalySimulation(new Random(),szserver.world.NativeAnomalyDefaults.damage(anomalyResources));
        fpvMap=new szserver.world.MapObstructionAccess(world,terrain,fpvSettings.boxes(),fpvSettings.baseBytes(),fpvSettings.editedBytes(),regions==null?szserver.world.SceneRegistry.load(registry.resolveSibling("active-stock-gameobjects.json")).names():regions.collisionDefinitions());rollback.push(fpvMap);zombies=new ZombieHorde(world,commands,fpvMap);fpvReceipts=new FpvReceipts(data);rollback.push(fpvReceipts);
        if(world.beforeStart())world.bootstrap(w->{w.actorMaps(fpvMap);return true;});else world.submit(w->{w.actorMaps(fpvMap);return true;}).get(5,TimeUnit.SECONDS);
        locations=new CharacterLocations(data,regions==null?"flat:arena-v1":regions.identity());rollback.push(locations);warps=new szserver.persistence.WarpStore(data);rollback.push(warps);environment=commands.environment();environmentAccess=new EnvironmentAccess(environment,new szserver.persistence.WeatherConfigurations(data,runtime));rollback.push(environmentAccess);
        packets=new GamePackets(root.resolve("fixtures/world-ready.b64"));nativePackets=new ClientPackets(runtime);rollback.push(nativePackets);nativePackets.input(Wire.bytes(o->{o.writeByte(7);o.writeFloat(0);o.writeInt(0);o.write(new byte[8]);o.writeInt(0);o.writeDouble(.5);o.writeDouble(1);o.writeDouble(.5);o.writeFloat(0);o.writeFloat(0);o.writeInt(1);o.writeInt(0);o.writeInt(1);o.writeFloat(.05f);o.writeByte(0);}),false);
        fpvExplosion=new FpvExplosion(root.resolve("fixtures/fpv-fab100.bin"));nativePackets.roundTrip(FpvExplosion.PACKET,fpvExplosion.body(new FpvPackets.Vector(0,64,0)));staminaRules=nativePackets.staminaRules();terrainProjection=new TerrainProjection(world,regions,packets,256);rollback.push(terrainProjection);nativeBlocks=new NativeBlockAccess(world,regions,terrainProjection,commands);rollback.push(nativeBlocks);nativePackets.roundTrip("146340354",new byte[20]);
        chunkManifests=new ChunkManifests(world,regions,packets);rollback.push(chunkManifests);nativePackets.roundTrip("1980173742",ChunkManifestPackets.body(List.of()));nativePackets.roundTrip("297896610",ChunkManifestPackets.body(List.of()));szserver.client.ChunkDistancePackets.warm(nativePackets,chunkSettings.distance());
        // No factory construction/codec cold path in the transport owner.
        try(GameItems items=new GameItems(runtime,registry)){byte[] empty=items.create(20004,new JsonObject());for(int n=0;n<=30;n++)baseMagazines[n]=items.magazine(empty,12714,n);}
        for(var e:com.google.gson.JsonParser.parseString(Files.readString(root.resolve("fixtures/equipment-appearances.json"))).getAsJsonObject().entrySet())appearances.put(Integer.parseInt(e.getKey()),HexFormat.of().parseHex(e.getValue().getAsString()));inventories=new GameInventory(root,data,runtime,registry);rollback.push(inventories);itemWorld=regions==null?"flat:arena-v1":regions.identity();worldItems=new szserver.world.WorldItems(fpvMap,itemWorld,entityIds::actor);rollback.push(worldItems);if(fpvSettings.toggleItemId()!=0)inventories.catalog().get(fpvSettings.toggleItemId());GrenadePackets.verifyRelease(root,runtime);LauncherPackets.verifyRelease(root,runtime);UnderbarrelPackets.verifyRelease(root,runtime);
        for(var e:inventories.explosives().entrySet())try{var definition=inventories.catalog().get(e.getKey()).properties();blastRules.put(e.getKey(),nativePackets.blastRules(definition.has("explosive_props")?definition.getAsJsonObject("explosive_props"):definition));var areas=nativePackets.grenadeAreas(e.getKey(),definition);if(!areas.isEmpty()){grenadeAreaProfiles.put(e.getKey(),areas);throw new IllegalArgumentException("grenade_area_gameplay_pending");}var profile=nativePackets.grenadeProfile(definition);if(e.getValue().radius()>128)throw new IllegalArgumentException("grenade_radius_budget");var use=nativePackets.grenadeUse(e.getKey(),definition);grenadeProfiles.put(e.getKey(),profile);grenadeUseRules.put(e.getKey(),use);}catch(IllegalArgumentException unsupported){grenadeUnavailable.put(e.getKey(),unsupported.getMessage());}
        SicknessPackets.verify(root,runtime);try(ClientSceneCodec codec=new ClientSceneCodec(runtime,root.resolve("fixtures/native-resources"))){sicknessRules=codec.sicknessRules();areaRelations=codec.areaRelations();continuousEffectKind=codec.grenadeEffectKind(false);instantEffectKind=codec.grenadeEffectKind(true);}AreaRelations.verify(root,runtime);for(var profile:sicknessRules.profiles())nativePackets.roundTrip(SicknessPackets.UPDATE,SicknessPackets.body(profile,sicknessRules.empty(profile.kind()),0,0));
        inventories.pulseProfiles(grenadeAreaProfiles);
        Set<Integer> effectOnlyAreas=new HashSet<>();for(int id:List.of(12359,12404)){var definition=inventories.catalog().get(id).properties();if(definition.has("has_effect")&&definition.get("has_effect").getAsBoolean()&&!definition.has("effect_aoe_object")&&!definition.has("effect_aoe")&&!definition.has("effects_aoe"))effectOnlyAreas.add(id);}
        grenadeAreaScenes=new GrenadeAreaScenes(root,runtime,registry,fpvSettings.boxes(),regions==null?Map.of():regions.collisionDefinitions(),grenadeAreaProfiles,inventories.explosives(),regions==null?null:regions.sceneResources(),effectOnlyAreas);rollback.push(grenadeAreaScenes);
        for(int item:grenadeAreaScenes.items())try{var definition=inventories.catalog().get(item).properties();var profile=nativePackets.grenadeProfile(definition);var use=nativePackets.grenadeUse(item,definition);grenadeProfiles.put(item,profile);grenadeUseRules.put(item,use);enabledAreaProfiles.put(item,grenadeAreaProfiles.get(item));grenadeUnavailable.remove(item);}catch(IllegalArgumentException unsupported){grenadeUnavailable.put(item,unsupported.getMessage());}
        nativePackets.roundTrip(GrenadeAreaPackets.REMOVE,Wire.bytes(out->out.writeInt(Integer.MIN_VALUE)));
        grenadeOutbox=new GrenadeOutbox(data,regions==null?"flat:arena-v1":regions.identity());rollback.push(grenadeOutbox);inventories.repairGrenadeReceipts(grenadeOutbox);
        grenadeAreas=new GrenadeAreaStore(data,regions==null?"flat:arena-v1":regions.identity());rollback.push(grenadeAreas);grenadeCompletion=new GrenadeCompletion(grenadeOutbox,grenadeAreas);grenadeCompletion.recover(System.currentTimeMillis());grenadeBridge=new GameGrenades(inventories,world,grenadeOutbox,grenadeAreas,enabledAreaProfiles);
        combatBarriers=new CombatBarrierQueue((from,to,at)->grenadeAreaScenes.shieldContact(grenadeAreas.states(),from,to,at),grenadeAreas::hitShield);
        for(var area:grenadeAreas.states())grenadeAreaScenes.validate(area);
        for(var restored:grenadeOutbox.states()){
            var receipt=restored.receipt();var profile=grenadeProfiles.get(receipt.item());var explosive=inventories.explosives().get(receipt.item());
            if(profile==null||explosive==null)throw new IllegalArgumentException("grenade_restore_unsupported_item:"+receipt.item());
            if(world.beforeStart())world.bootstrap(w->{w.grenades().restore(receipt.character().toString(),explosive,receipt.flight(),restored.saved());return true;});
            else world.submit(w->{w.grenades().restore(receipt.character().toString(),explosive,receipt.flight(),restored.saved());return true;}).get(5,TimeUnit.SECONDS);
            thrownGrenades.put(receipt.projectile(),new ThrownGrenade(null,receipt.character(),receipt.name(),receipt.ownerContext(),-1,profile));grenadeRecovered++;
        }
        nativePackets.roundTrip(GrenadePackets.START,new byte[4]);nativePackets.roundTrip(GrenadePackets.THROW,new byte[16]);nativePackets.roundTrip(GrenadePackets.UPDATE,new byte[28]);nativePackets.roundTrip(GrenadePackets.PREDICTION,new byte[8]);
        medicines=inventories.medicines();statOrdinals=inventories.statOrdinals();warmAppearances();accounts=new LocalAccounts(httpPort,routes,data.resolve("local-accounts.json"),inventories::lobbyPreview,inventories::ensureDefaults,false);rollback.push(accounts);gateway=new ServerSocket();rollback.push(gateway);gateway.bind(new InetSocketAddress(routes.bindAddress(),gatewayPort),32);udp=new DatagramSocket(null);rollback.push(udp);udp.bind(new InetSocketAddress(routes.bindAddress(),udpPort));udp.setReceiveBufferSize(4*1024*1024);udp.setSoTimeout(2);
        CreativeGiveHooks.bind(this);
        adminInventories=new AdminInventories(data,inventories,accounts::characterExists,this::adminTask,this::syncAdminInventory);rollback.push(adminInventories);commands.bindPlayerInventories(adminInventories);
        rollback.push(()->commands.unbindPlayerInventories(adminInventories));
        characterDeletions=new CharacterDeletions(inventories,locations,this::adminTask,id->{for(Session active:new ArrayList<>(sessions.values()))if(active.grant!=null&&id.equals(active.grant.characterId()))remove(active);});rollback.push(characterDeletions);
        accounts.bindCharacterDeletion(characterDeletions::prepare);
        accounts.recoverCharacterDeletions(id->inventories.deleteOffline(id).thenCompose(ignored->locations.deleteCharacter(id))).get(30,TimeUnit.SECONDS);
        inventories.repairVitalsReceipts(locations);
        acceptor=new Thread(this::accept,"gateway-accept");acceptor.setDaemon(true);transport=new Thread(this::run,"native-game-owner");transport.setDaemon(true);started.add(acceptor);started.add(transport);if(autoStart)start();
        System.out.println("Native arena HTTP="+httpPort+" TCP="+gatewayPort+" UDP="+udpPort);
        rollback.clear();
        }catch(Exception|Error failure){running=false;while(!rollback.isEmpty())try{rollback.pop().close();}catch(Throwable cleanup){if(cleanup!=failure)failure.addSuppressed(cleanup);}for(Thread thread:started)try{thread.join(2000);}catch(InterruptedException cleanup){Thread.currentThread().interrupt();failure.addSuppressed(cleanup);}throw failure;}
    }
    public synchronized void start()throws IOException{
        if(closed||transport.getState()!=Thread.State.NEW)throw new IllegalStateException("game_already_started_or_closed");
        if(world.beforeStart())world.start();running=true;
        try{acceptor.start();transport.start();accounts.start();}
        catch(RuntimeException|Error failure){running=false;for(AutoCloseable socket:List.of(gateway,udp))try{socket.close();}catch(Exception cleanup){failure.addSuppressed(cleanup);}throw failure;}
    }
    public Map<String,Object> metrics(){var view=new LinkedHashMap<String,Object>(metrics);view.put("running",running);view.put("closed",closed);view.put("listenerPorts",Map.of("http",accounts.port(),"gateway",gateway.getLocalPort(),"world",udp.getLocalPort()));view.put("adminInventory",adminInventories.metrics());return Collections.unmodifiableMap(view);}
    public LocalAccounts developmentAccounts(){return accounts;}
    public CharacterLocations developmentLocations(){return locations;}
    public boolean healthy(){return running&&!closed&&environmentAccess.configurations.healthy()&&grenadeAreaScenes.healthy()&&grenadeBridge.healthy()&&acceptor.isAlive()&&transport.isAlive()&&accounts.healthy()&&locations.healthy()&&inventories.healthy()&&adminInventories.healthy()&&warps.healthy()&&environment.healthy()&&fpvMap.healthy()&&fpvReceipts.healthy()&&(regions==null||regions.healthy());}
    public boolean ready(){return healthy()&&fpvMap.ready()&&(regions==null||regions.ready());}
    private void adminTask(Runnable task){if(!running)throw new RejectedExecutionException("game_owner_stopped");if(!adminTasks.offer(task))throw new RejectedExecutionException("game_admin_queue_full");}
    private void drainAdminTasks(){screenshots.advance();for(int n=0;n<16;n++){Runnable task=adminTasks.poll();if(task==null)break;task.run();}}
    private void syncAdminInventory(UUID id,Inventory.Plan plan){for(Session s:List.copyOf(sessions.values()))if(s.ready&&character(s).equals(id))try{
        // The player can move/use an item while the receipt worker is fsyncing.
        // Never publish an older point delta over a newer acknowledged inventory.
        if(inventories.inventory(id).revision()==plan.after().revision())inventoryChanged(s,plan,()->{});
        else{inventorySnapshot(s,0);refreshReserve(s);}
    }catch(IllegalStateException slow){remove(s);}}
    private void accept(){while(running)try{Socket socket=gateway.accept();try{logins.execute(()->login(socket));}catch(RejectedExecutionException e){socket.close();}}catch(IOException e){if(running)System.err.println("gateway_accept: "+e.getMessage());}}
    private void login(Socket socket){try(socket){socket.setSoTimeout(5000);socket.setTcpNoDelay(true);byte[] frame=Wire.readFrame(socket.getInputStream());ByteBuffer b=ByteBuffer.wrap(frame);if(Wire.varint(b)!=1)throw new IOException("login_id");var in=Wire.input(Arrays.copyOfRange(frame,b.position(),frame.length));UUID protocol=new UUID(in.readLong(),in.readLong());in.readByte();String login=in.readUTF(),name=in.readUTF(),session=in.readUTF(),token=in.readUTF();LocalAccounts.Grant grant=accounts.gateway(protocol,login,name,session,token);socket.getOutputStream().write(Wire.frame(3,new byte[0]));boolean complete=false;for(int i=0;i<16;i++){byte[] request=Wire.readFrame(socket.getInputStream());if(Wire.varint(ByteBuffer.wrap(request))==3){complete=true;break;}}if(!complete)throw new IOException("redirect_request");socket.getOutputStream().write(Wire.frame(10200,Wire.bytes(o->{o.writeUTF(grant.worldHost());o.writeInt(routes.worldPort());o.writeUTF(GamePackets.WORLD_KEY);})));}catch(Exception e){System.err.println("Gateway rejected: "+e.getMessage());}}
    private void run(){
        ownerStarted=System.nanoTime();long deadline=ownerStarted;byte[] buffer=new byte[65535];
        while(running){
            long waitBegin=System.nanoTime(),socketWait=0,receiveWork=0;
            try{var datagram=new DatagramPacket(buffer,buffer.length);udp.receive(datagram);socketWait=System.nanoTime()-waitBegin;long received=System.nanoTime();try{receive((InetSocketAddress)datagram.getSocketAddress(),Arrays.copyOf(buffer,datagram.getLength()));}finally{receiveWork=System.nanoTime()-received;if(receiveWork>=10_000_000L)slowPacket("datagram",datagram.getLength()>4?buffer[4]&255:-1,datagram.getLength(),receiveWork);}}
            catch(SocketTimeoutException ignored){socketWait=System.nanoTime()-waitBegin;}
            catch(Exception e){rejected++;if(rejected<=5)System.err.println("native packet rejected: "+e);}
            maxSocketWaitNanos=Math.max(maxSocketWaitNanos,socketWait);maxReceiveNanos=Math.max(maxReceiveNanos,receiveWork);
            drainApplications(deadline);
            long completionBegin=System.nanoTime();inventories.drain();grenadeBridge.drain();advanceGrenades();advanceMines();advanceGrenadeAreas();advanceVitals();advancePlayerStates();drainAdminTasks();characterDeletions.advance();advanceFpvPlacements();drainCombat();long now=System.nanoTime(),completionWork=now-completionBegin;maxCompletionNanos=Math.max(maxCompletionNanos,completionWork);
            if(now>=deadline){
                long late=now-deadline,gap=lastTickBegin==0?0:now-lastTickBegin;lastTickBegin=now;maxTickLateNanos=Math.max(maxTickLateNanos,late);maxTickGapNanos=Math.max(maxTickGapNanos,gap);
                long begin=now;try{step(now);}catch(Exception e){System.err.println("native tick failed: "+e);e.printStackTrace();running=false;}long work=System.nanoTime()-begin;maxTickNanos=Math.max(maxTickNanos,work);
                deadline+=50_000_000;if(now>=deadline){long missed=(now-deadline)/50_000_000+1;skipped+=missed;deadline+=missed*50_000_000;
                    if(lateTicks.size()==8)lateTicks.removeFirst();lateTicks.addLast(Map.of("tick",tick,"uptimeMs",(now-ownerStarted)/1e6,"lateMs",late/1e6,"gapMs",gap/1e6,"socketWaitMs",socketWait/1e6,"receiveMs",receiveWork/1e6,"completionMs",completionWork/1e6,"tickMs",work/1e6,"missedSlots",missed));
                }
            }
        }
    }
    private void sendRaw(Session s,byte[] bytes)throws IOException{udp.send(new DatagramPacket(bytes,bytes.length,s.address));}
    private void enqueueOrRetire(Session s,byte[] stream){if(sessions.get(s.address)!=s)return;try{enqueue(s,0,stream);}catch(IllegalStateException slow){remove(s);}}
    private void reliable(Session s,int lane,int kind,int sequence,int time,byte[] payload)throws IOException{sendRaw(s,Wire.bytes(o->{o.writeInt(s.id);o.writeByte(lane);o.writeByte(kind);o.writeByte(0);o.writeShort(WINDOW);o.writeInt(time);o.writeInt(sequence);o.writeInt(s.lanes[lane].rx);o.writeInt((kind&7)==1?payload.length:1);o.write(payload);}));}
    private void enqueue(Session s,int lane,byte[] stream){if(s.vehicleCapable)stream=VehicleEnvelope.current(stream);Lane l=s.lanes[lane];int count=(stream.length+FRAGMENT-1)/FRAGMENT;if(l.queued.size()+count>MAX_QUEUE)throw new IllegalStateException("reliable_output_limit");for(int i=0;i<stream.length;i+=FRAGMENT)l.queued.add(Arrays.copyOfRange(stream,i,Math.min(stream.length,i+FRAGMENT)));}
    private void flush(Session s,long now)throws IOException{for(int lane=0;lane<2;lane++){Lane l=s.lanes[lane];int budget=16;for(var e:l.sent.entrySet()){Sent sent=e.getValue();if(now-sent.last>200_000_000L&&budget-->0){if(sent.attempts>=40)throw new IOException("reliable_stalled");reliable(s,lane,129,e.getKey(),(int)(now/1_000_000),sent.payload);e.setValue(new Sent(sent.payload,now,sent.attempts+1));retries++;}}while(budget-->0&&l.sent.size()<WINDOW&&!l.queued.isEmpty()){byte[] payload=l.queued.remove();int seq=l.tx++;reliable(s,lane,1,seq,(int)(now/1_000_000),payload);l.sent.put(seq,new Sent(payload,now,1));}}}
    private void receive(InetSocketAddress address,byte[] raw)throws Exception{if(raw.length<5)throw new IOException("datagram_header");ByteBuffer b=ByteBuffer.wrap(raw);int connection=b.getInt(),type=b.get()&255;datagrams.merge(type,1L,Long::sum);Session s=sessions.get(address);
        if(type==20){if(raw.length<6||raw.length>512||b.get()!=1)throw new IOException("hello_version");if(s!=null&&s.clientId==connection&&!s.accepted){challenge(s);return;}if(s!=null)remove(s);if(sessions.size()>=MAX_SESSIONS)return;long began=System.nanoTime();byte[] cookie=new byte[20];random.nextBytes(cookie);int id=random.nextInt();long cookieEnd=System.nanoTime();s=new Session(address,id,connection,cookie,chunkSettings);sessions.put(address,s);long sessionEnd=System.nanoTime();challenge(s);long challengeEnd=System.nanoTime();maxHelloCookieNanos=Math.max(maxHelloCookieNanos,cookieEnd-began);maxHelloSessionNanos=Math.max(maxHelloSessionNanos,sessionEnd-cookieEnd);maxHelloChallengeNanos=Math.max(maxHelloChallengeNanos,challengeEnd-sessionEnd);return;}
        if(s==null||s.id!=connection){connectionMismatch++;return;}s.last=System.nanoTime();
        if(type==17){if(b.remaining()!=25||b.get()!=20)throw new IOException("cookie_size");byte[] cookie=new byte[20];b.get(cookie);int nonce=b.getInt();if(!java.security.MessageDigest.isEqual(cookie,s.cookie))throw new IOException("cookie_invalid");s.accepted=true;Session target=s;sendRaw(s,Wire.bytes(o->{o.writeInt(target.id);o.writeByte(18);o.writeInt(nonce+1);o.writeInt(0);}));return;}
        if(!s.accepted)return;if(type==16){remove(s);return;}if(type==22||type==23)return;if(type!=0&&type!=1)return;Lane l=s.lanes[type];
        while(b.hasRemaining()){if(b.remaining()<20)throw new IOException("reliable_header");int kind=b.get()&7;b.get();b.getShort();int time=b.getInt(),seq=b.getInt(),peerRx=b.getInt(),value=b.getInt();
            l.sent.entrySet().removeIf(e->Integer.compareUnsigned(peerRx-e.getKey(),1)>=0&&Integer.compareUnsigned(peerRx-e.getKey(),4096)<0);
            if(kind==2){if(value<0||value>WINDOW)throw new IOException("ack_range");for(int i=0;i<value;i++)l.sent.remove(seq+i);continue;}
            if(kind!=1||value<0||value>b.remaining()||value>8192)throw new IOException("reliable_data");byte[] payload=new byte[value];b.get(payload);
            int distance=seq-l.rx;if(Integer.compareUnsigned(distance,WINDOW)>=0){if(distance<0)reliable(s,type,2,seq,time,new byte[0]);continue;}
            l.received.putIfAbsent(seq,payload);reliable(s,type,2,seq,time,new byte[0]);while(l.received.containsKey(l.rx)){byte[] part=l.received.remove(l.rx++);l.stream.append(part);if(!l.parsing){l.parsing=true;parseReady.addLast(new ParseTarget(s,type));}}
        }
    }
    private static byte[] challengeBytes(Session s){return Wire.bytes(o->{o.writeInt(s.clientId);o.writeByte(21);o.writeByte(0);o.writeByte(1);o.writeInt(s.id);o.writeByte(20);o.write(s.cookie);});}
    private void challenge(Session s)throws IOException{sendRaw(s,challengeBytes(s));}
    private void slowPacket(String phase,int id,int bytes,long nanos){
        if(slowPackets.size()==8)slowPackets.removeFirst();slowPackets.addLast(Map.of("phase",phase,"id",id,"bytes",bytes,"ms",nanos/1e6,"uptimeMs",(System.nanoTime()-ownerStarted)/1e6));
    }
    private void drainApplications(long tickDeadline){
        // One message per lane turn: a busy client cannot monopolize accumulated frames.
        // This budget cannot preempt a single handler; slow-handler records expose that case.
        long until=Math.min(tickDeadline,System.nanoTime()+2_000_000L);int budget=64;
        while(!parseReady.isEmpty()&&budget-->0&&System.nanoTime()<until){
            ParseTarget target=parseReady.removeFirst();Session s=target.session();Lane lane=s.lanes[target.lane()];
            if(sessions.get(s.address)!=s){lane.parsing=false;continue;}
            ReliableInput.Message message;
            try{message=lane.stream.next();}catch(IOException error){lane.parsing=false;rejected++;remove(s);continue;}
            if(message==null){lane.parsing=false;continue;}
            long begin=System.nanoTime();
            try{application(s,message.id(),message.body());}catch(Exception error){rejected++;if(rejected<10)System.err.println("message "+message.id()+" rejected: "+error);}
            finally{long work=System.nanoTime()-begin;maxApplicationNanos=Math.max(maxApplicationNanos,work);if(work>=10_000_000L)slowPacket("application",message.id(),message.body().length,work);}
            if(sessions.get(s.address)==s)parseReady.addLast(target);else lane.parsing=false;
        }
        if(!parseReady.isEmpty())applicationBudgetYields++;
    }
    private void application(Session s,int id,byte[] body)throws Exception{
        if(s.ready&&NpcEditorHooks.application(this,s,id,body))return;
        if(applicationCounts.containsKey(id)||applicationCounts.size()<512)applicationCounts.merge(id,1L,Long::sum);
        if(id==7){enqueue(s,0,Wire.frame(10,new byte[0]));return;}
        if(id==8){if(s.grant!=null||s.authentication!=null)return;var in=Wire.input(body);UUID protocol=new UUID(in.readLong(),in.readLong());String ticket=in.readUTF();in.readInt();if(in.available()!=0)throw new IOException("login_body");
            s.loginProtocol=protocol;s.authenticationOrder=Math.incrementExact(authenticationOrder);authenticationOrder=s.authenticationOrder;CompletableFuture<LocalAccounts.Grant> pending=new CompletableFuture<>();s.authentication=pending;
            try{logins.execute(()->{try{pending.complete(accounts.claim(protocol,ticket));}catch(Exception error){pending.completeExceptionally(error);}});}catch(RejectedExecutionException full){pending.completeExceptionally(full);}return;
        }
        if(!s.ready)return;
        if(id==1501)id=5; // Current native dynamic envelope (legacy peers use 5).
        if(id==5){var dynamic=Wire.input(body);int registered=GameplayPacketAliases.registered(dynamic.readUTF());if(registered!=0){id=registered;body=dynamic.readAllBytes();}}
        if(id==ScreenshotPackets.RESPONSE){screenshots.receive(s.incarnation,body);return;}
        if(id==ScreenshotPackets.ENVELOPE){byte[] capture=ScreenshotPackets.envelope(body);if(capture!=null){screenshots.receive(s.incarnation,capture);return;}}
        if(id==1070){grenadeStart(s,GrenadePackets.start(body));return;}
        if(id==973){grenadeThrow(s,GrenadePackets.read(body));return;}
        if(id==1289){nativeAmmo(s,body);return;}
        if(id==1437){s.adminSelected=NativeAdminPackets.address(body);return;}
        if(id==1719){s.creativeSelected=NativeAdminPackets.selected(body);s.adminSelected=null;return;}
        if(id==1033){anomalyVelocityEcho(body);return;}
        if(id==1338){anomalyForceEcho(body);return;}
        if(id==747){anomalyStrengthSave(s,body);return;}
        if(id==559){anomalyConfigurationSave(s,body);return;}
        if(id==466){sceneConfiguration(s,body);return;}
        if(id==1191||id==1610){sceneConfigurationSave(s,body,id==1610);return;}
        String sceneKind=szserver.client.SceneEditPackets.kind(id);if(sceneKind!=null){sceneEdit(s,sceneKind,body);return;}
        if(id==1481){nativeBlockIntents++;if(szserver.client.PortalPackets.emptyHand(body))requestPortal(s,true);return;}
        if(id==167){NativeBlockPackets.target(body,false);nativeBlockIntents++;return;}
        if(id==2013){nativeDestroy(s,body);return;}
        if(szserver.client.TeleportPackets.mapId(id,body.length)){try{teleport(s,TeleportCommands.map(body),"Телепортация по карте выполнена.");}catch(IllegalArgumentException error){enqueue(s,0,GamePackets.chat(error.getMessage()));}return;}
        if(id==1022){meleeSwing(s,body);return;}
        if(id==1435){meleeHit(s,body);return;}
        if(id==WorldItemPackets.DROP_ID){dropItem(s,WorldItemPackets.drop(body));return;}
        if(id==693){var in=Wire.input(body);int owner=in.readShort(),sequence=in.readShort();Inventory.Address address=nullableAddress(in);if(owner!=0||address!=null&&(address.aggregate()!=1||address.container()!=0)||in.available()!=0)throw new IOException("medicine_inventory_request");ItemStack selected=address==null?null:inventory(s).at(address);if(selected!=null&&inventories.lootboxes().isBox(selected.id())){lootboxPreview(s,selected.id());enqueue(s,0,InventoryPackets.ack(owner,sequence));}else medicine(s,address==null?null:address.slot(),()->enqueue(s,0,InventoryPackets.ack(owner,sequence)));return;}
        if(id==1741){var in=Wire.input(body);Inventory.Address address=new Inventory.Address(in.readUnsignedByte(),in.readUnsignedByte(),in.readUnsignedShort());int owner=in.readShort(),sequence=in.readShort();if(owner!=0||address.aggregate()!=1||address.container()!=0||in.available()!=0)throw new IOException("lootbox_open_all_request");ItemStack selected=inventory(s).at(address);if(selected==null||!inventories.lootboxes().isBox(selected.id())){enqueue(s,0,InventoryPackets.ack(owner,sequence));return;}int count=Math.min(500,LootboxOpening.count(inventory(s),selected.id()));if(s.hp<=0||s.respawning){enqueue(s,0,InventoryPackets.ack(owner,sequence));return;}inventories.openLootboxes(character(s),selected.id(),count,result->{inventoryChanged(s,result.plan(),()->enqueue(s,0,InventoryPackets.ack(owner,sequence)));enqueue(s,0,GamePackets.chat("Открыто ящиков: "+count+". Добыча добавлена в рюкзак."));},error->{inventorySnapshot(s,0);enqueue(s,0,InventoryPackets.ack(owner,sequence));enqueue(s,0,GamePackets.chat(lootboxError(error)));});return;}
        if(id==695){if(body.length!=16)throw new IOException("lootbox_finish_request");return;}
        if(id==928){if(body.length!=4||ByteBuffer.wrap(body).getInt()!=-1)throw new IOException("inventory_open");openInventory(s);return;}
        if(id==2060||id==562){if(id==2060&&body.length!=0||id==562&&body.length!=4)throw new IOException("storage_open");int page=id==562?ByteBuffer.wrap(body).getInt():s.inventoryScreen.page();if(page<0||page>=InventoryScreen.PAGES)throw new IOException("storage_page");openStorage(s,page,id==2060);return;}
        if(id==InventoryPackets.SORT_ID){inventorySort(s,body);return;}
        if(id==1231||id==102||id==1627||id==2040||id==2009){inventoryRequest(s,id,body);return;}
        if(id==212){if(!playerRules.entitled(character(s))){inventorySnapshot(s,0);if(body.length>=4&&ByteBuffer.wrap(body).getShort()==0)enqueue(s,0,InventoryPackets.ack(0,ByteBuffer.wrap(body).getShort(2)));enqueue(s,0,GamePackets.chat("item_editor_requires_admin"));return;}if(body.length<18||body.length>8200)throw new IOException("editor_request_size");var in=Wire.input(body);int owner=in.readShort(),sequence=in.readShort();Inventory.Address address=new Inventory.Address(in.readUnsignedByte(),in.readUnsignedByte(),in.readUnsignedShort());if(owner!=0||address.aggregate()==2)throw new IOException("editor_owner");inventories.edit(character(s),address,in.readAllBytes(),plan->inventoryChanged(s,plan,()->enqueue(s,0,InventoryPackets.ack(owner,sequence))),error->{inventorySnapshot(s,0);enqueue(s,0,InventoryPackets.ack(owner,sequence));enqueue(s,0,GamePackets.chat(error));});return;}
        if(id==1527){if(!playerRules.entitled(character(s))){inventorySnapshot(s,0);enqueue(s,0,GamePackets.chat("item_editor_requires_admin"));return;}if(body.length<10||body.length>65536||body[0]!=1)throw new IOException("editor_config_size");var in=Wire.input(body);in.readByte();Inventory.Address address=new Inventory.Address(in.readUnsignedByte(),in.readUnsignedByte(),in.readUnsignedShort());int size=in.readInt();if(address.aggregate()==2||size<=0||size!=in.available())throw new IOException("editor_config_layout");inventories.edit(character(s),address,in.readAllBytes(),true,plan->inventoryChanged(s,plan,()->{}),error->{inventorySnapshot(s,0);enqueue(s,0,GamePackets.chat(error));});return;}
        if(id==366||id==598){var in=Wire.input(body);int owner,sequence,slot=0,selection=0;Inventory.Address target,source;
            if(id==366){slot=in.readInt();target=nullableAddress(in);int absent=in.readUnsignedByte();if(absent>1)throw new IOException("paint_source_presence");source=absent==1?null:nullableAddress(in);selection=in.readInt();owner=in.readShort();sequence=in.readShort();}
            else{owner=in.readShort();sequence=in.readShort();target=nullableAddress(in);source=nullableAddress(in);}
            if(owner!=0||target==null||target.aggregate()==2||source!=null&&source.aggregate()!=1||selection< -1||slot<0||slot>1||in.available()!=0)throw new IOException("cosmetic_request");
            if(id==366&&source==null&&selection>0&&!playerRules.entitled(character(s))){inventorySnapshot(s,0);enqueue(s,0,InventoryPackets.ack(owner,sequence));enqueue(s,0,GamePackets.chat("paint_editor_requires_admin"));return;}
            inventories.cosmetic(character(s),target,source,selection,slot,id==598,plan->inventoryChanged(s,plan,()->enqueue(s,0,InventoryPackets.ack(owner,sequence))),error->{inventorySnapshot(s,0);enqueue(s,0,InventoryPackets.ack(owner,sequence));enqueue(s,0,GamePackets.chat(error));});return;}
        if(id==UnderbarrelPackets.UNLOAD_ID){unloadLauncher(s,UnderbarrelPackets.unload(body));return;}
        if(id==1449){var in=Wire.input(body);int owner=in.readShort(),sequence=in.readShort();Inventory.Address target=nullableAddress(in),source=nullableAddress(in);int selection=in.readInt(),slot=in.readUnsignedByte();if(owner!=0||target==null||target.aggregate()==2||source!=null&&source.aggregate()==2||selection<0||slot>=18||in.available()!=0)throw new IOException("module_request");inventories.module(character(s),target,source,selection,slot,plan->inventoryChanged(s,plan,()->enqueue(s,0,InventoryPackets.ack(owner,sequence))),error->{inventorySnapshot(s,0);enqueue(s,0,InventoryPackets.ack(owner,sequence));enqueue(s,0,GamePackets.chat(error));});return;}
        if(id==1138){var in=Wire.input(body);int owner=in.readShort(),sequence=in.readShort();Inventory.Address target=nullableAddress(in),source=nullableAddress(in);int slot=in.readInt();if(owner!=0||!new Inventory.Address(0,3,0).equals(target)||source!=null&&source.aggregate()!=1||in.available()!=0)throw new IOException("artifact_socket_request");inventories.socket(character(s),source,slot,plan->inventoryChanged(s,plan,()->enqueue(s,0,InventoryPackets.ack(owner,sequence))),error->{inventorySnapshot(s,0);enqueue(s,0,InventoryPackets.ack(owner,sequence));enqueue(s,0,GamePackets.chat(error));});return;}
        if(id==1060){receiveChat(s,body);return;}
        if(id==10203){enqueue(s,0,Wire.frame(10204,body));return;}
        if(id==1345||id==1978){if(s.respawning)return;ClientPackets.Input input=nativePackets.input(body,id==1978);if(input.tick()<s.clientTick)return;s.clientTick=input.tick();if(vehicles.driving(s)!=null){if(input.yaw()!=null){s.yaw=input.yaw();s.pitch=input.pitch();}if(vehicleActions.windowView(s,System.nanoTime())){s.animation.input(input.motion());inputButtons(s,input.buttons());}s.lastInput=System.nanoTime();inputs++;return;}if(!input.blockBatch().deltas().isEmpty())nativeBlockBatch(s,input.blockBatch());boolean acceptedPosition=false;if(input.position()!=null){double[] pos=input.position();if(Math.abs(pos[0])<=30_000_001&&Math.abs(pos[2])<=30_000_001&&Math.abs(pos[1])<=30_000_001){if(!s.scaleSaving&&s.scaleChange==null&&s.teleport==null&&s.teleportGuard.accept(point(pos),System.nanoTime())){s.pos=pos;acceptedPosition=true;requestPortal(s,false);}else staleTeleportInputs++;}}if(input.yaw()!=null){s.yaw=input.yaw();s.pitch=input.pitch();}s.motion.input(input,acceptedPosition);s.posture.input(input,s.motion.ground());if(input.mode()==5&&s.mode!=5&&s.hp>0)s.stamina=staminaRules.enterSprint(s.stamina);s.mode=input.mode();s.lastInput=System.nanoTime();if(input.stamina()!=null){s.reportedStamina=input.stamina();s.observedStamina=input.stamina();}s.speed=s.motion.speed();s.velocity=s.motion.velocity();s.ground=s.motion.ground();s.animation.input(input.motion());inputButtons(s,input.buttons());inputs++;return;}
        if(id==1356){for(var entry:ChunkManifestPackets.requests(body)){if(entry.release())releaseChunk(s,chunkKey(entry.x(),entry.z()));else demandChunk(s,entry);}return;}
        if(id==240){if(body.length==1&&body[0]>=1&&body[0]<=3)enqueue(s,0,Wire.dynamic("1619370828",body));return;}if(id==2059){selectHand(s,szserver.client.HandPackets.read(body));return;}if(id==271){shot(s,body);return;}if(id==UnderbarrelPackets.ID){UnderbarrelPackets.read(body);launchUnderbarrel(s);return;}if(id==LauncherPackets.ID){LauncherPackets.read(body);launchPrimary(s);return;}
        if(id==WeaponJammingPackets.ID){WeaponJammingPackets.read(body);queueUnjam(s);return;}
        if(id==280){if(body.length!=0&&body.length!=5)throw new IOException("reload_request_size");int ammo=body.length==0?0:ByteBuffer.wrap(body).getInt();if(ammo<0||ammo>65535||body.length==5&&(body[4]<0||body[4]>1))throw new IOException("reload_request_ammunition");if(body.length==5&&body[4]==1){queueLauncherReload(s,ammo);return;}queueReload(s,ammo);return;}
        if(id==1013){changeMode(s,body);return;}
        if(id==1661){requestRespawn(s,body);return;}
        if(id==5){var in=Wire.input(body);String cls=in.readUTF();if(cls.equals(playerscale.ScaleProtocol.INPUT)){scaleHello(s,in.readAllBytes());}else if(cls.equals(EnvironmentWire.INPUT)){try{environmentAccess.request(s.environmentClient,in.readAllBytes(),playerRules.editor(character(s),worldEditors));}catch(Exception failure){environmentAccess.disconnect(s.environmentClient);JsonObject reply=EnvironmentWire.message("ack");reply.addProperty("ok",false);reply.addProperty("error",Objects.toString(failure.getMessage(),failure.getClass().getSimpleName()));enqueue(s,0,EnvironmentAccess.frame(reply));}}else if(cls.equals(WeaponJammingPackets.REQUEST)){WeaponJammingPackets.read(in.readAllBytes());queueUnjam(s);}else if(cls.equals(UnderbarrelPackets.UNLOAD_REQUEST)){unloadLauncher(s,UnderbarrelPackets.unload(in.readAllBytes()));}else if(cls.equals(UnderbarrelPackets.REQUEST)){UnderbarrelPackets.read(in.readAllBytes());launchUnderbarrel(s);}else if(cls.equals(LauncherPackets.REQUEST)){LauncherPackets.read(in.readAllBytes());launchPrimary(s);}else if(cls.equals(GrenadePackets.START)){grenadeStart(s,GrenadePackets.start(in.readAllBytes()));}else if(cls.equals(GrenadePackets.THROW)){grenadeThrow(s,GrenadePackets.read(in.readAllBytes()));}else if(szserver.client.HandPackets.selectionClass(cls)){selectHand(s,szserver.client.HandPackets.read(in.readAllBytes()));}else if(NativeAdminPackets.giveClass(cls)){nativeGive(s,NativeAdminPackets.give(in.readAllBytes()));}else if(NativeAdminPackets.ammoClass(cls)){nativeAmmo(s,in.readAllBytes());}else if(NativeAdminPackets.addressClass(cls)){s.adminSelected=NativeAdminPackets.address(in.readAllBytes());}else if(NativeAdminPackets.echoClass(cls)){nativeEcho(s,in.readAllBytes());}else if(cls.equals(PhysicalReload.REQUEST)){physicalRequest(s,in.readAllBytes());}else if(cls.equals(CommandSupport.REQUEST)){completion(s,in.readAllBytes());}else if(cls.equals(Vehicles.INPUT)){vehicle(s,in.readAllBytes());}else if(FpvPackets.inputClass(cls)){fpv(s,in.readAllBytes());}else if(cls.equals("1618367864")){anomalyVelocityEcho(in.readAllBytes());}else if(cls.equals("804507492")){anomalyForceEcho(in.readAllBytes());}else if(cls.equals("1552498486")){anomalyStrengthSave(s,in.readAllBytes());}else if(cls.equals("1019975257")){anomalyConfigurationSave(s,in.readAllBytes());}else if(SceneConfigurationPackets.requestClass(cls)){sceneConfiguration(s,in.readAllBytes());}else if(SceneConfigurationPackets.saveClass(cls)){sceneConfigurationSave(s,in.readAllBytes(),false);}else if(SceneConfigurationPackets.settingsClass(cls)){sceneConfigurationSave(s,in.readAllBytes(),true);}else if(szserver.client.SceneEditPackets.kind(cls)!=null){sceneEdit(s,szserver.client.SceneEditPackets.kind(cls),in.readAllBytes());}else if(cls.equals("146340354")){nativeDestroy(s,in.readAllBytes());}else if(cls.equals("1056067431")){NativeBlockPackets.target(in.readAllBytes(),false);nativeBlockIntents++;}else if(szserver.client.PortalPackets.useClass(cls)){nativeBlockIntents++;if(szserver.client.PortalPackets.emptyHand(in.readAllBytes()))requestPortal(s,true);}else if(szserver.client.TeleportPackets.mapClass(cls)){try{teleport(s,TeleportCommands.map(in.readAllBytes()),"Телепортация по карте выполнена.");}catch(IllegalArgumentException error){enqueue(s,0,GamePackets.chat(error.getMessage()));}}else if(szserver.client.PermissionPackets.modeClass(cls)){changeMode(s,in.readAllBytes());}else if(cls.equals("1102124358")){requestRespawn(s,in.readAllBytes());}else if(cls.equals("677917411")){receiveChat(s,in.readAllBytes());}else if(cls.equals("szmods.bridge.MedicineUse")){byte[] request=in.readAllBytes();if(request.length>1||request.length==1&&request[0]!=1)throw new IOException("medicine_request");medicine(s,null);}return;}
        if(unknown.containsKey(id)||unknown.size()<128)unknown.merge(id,1L,Long::sum);if(unknown.size()>256)unknown.clear();
    }
    private void repairNativeBlock(Session s,World.Block p){long key=szserver.world.BlockOverlays.key(Math.floorDiv(p.x(),16),Math.floorDiv(p.z(),16));if(!s.blockRepairs.containsKey(key)&&s.blockRepairs.size()>=9)return;BlockRepair repair=s.blockRepairs.computeIfAbsent(key,k->new BlockRepair());if(repair.points.size()<32)repair.points.add(p);}
    private static byte[] vehicleFrame(VehicleWire state){return Wire.frame(1501,Wire.bytes(out->{out.writeUTF(Vehicles.SNAPSHOT);out.write(state.encode());}));}
    private void vehicle(Session s,byte[] body){
        VehicleWire request=null;
        try{
            request=VehicleWire.decode(body);long now=System.nanoTime();if(request.op!=VehicleWire.STATE&&request.op!=VehicleWire.CONTACT&&request.op!=VehicleWire.SEAT_HEARTBEAT)System.out.println("SC Vehicles lifecycle op="+request.op+" player="+s.entity);
            if(request.op==VehicleWire.HELLO){vehicles.seatSupport(s,request.sequence>=VehicleSeats.VERSION);s.vehicleInteractions=request.sequence>=VehicleWire.INTERACTION_VERSION;s.vehicleContacts=request.sequence>=VehicleWire.CONTACT_VERSION;s.vehicleArticulation=request.sequence>=VehicleWire.ARTICULATION_VERSION;s.vehicleFleet=request.sequence>=VehicleWire.FLEET_VERSION;VehicleWire hello=new VehicleWire();hello.op=VehicleWire.SNAPSHOT;hello.sequence=vehicles.seatSupport(s)?VehicleSeats.VERSION:s.vehicleInteractions?VehicleWire.INTERACTION_VERSION:s.vehicleContacts?VehicleWire.CONTACT_VERSION:s.vehicleFleet?VehicleWire.FLEET_VERSION:s.vehicleArticulation?VehicleWire.ARTICULATION_VERSION:0;enqueueOrRetire(s,vehicleFrame(hello));if(!s.vehicleCapable){s.vehicleCapable=true;
                // Early legacy-framed terrain may already have been acknowledged by the
                // transport but rejected by the native RPC decoder. Reissue those chunks
                // through the negotiated current envelope, without truncating either lane.
                for(long key:new ArrayList<>(s.chunks)){releaseChunk(s,key);chunk(s,(int)(key>>32),(int)key);}
                for(var car:vehicles.all())emitVehicleTo(s,car,VehicleWire.SNAPSHOT);if(s.vehicleContacts)for(var link:vehicleContacts.all())emitVehicleContactTo(s,link.owner(),link.state());if(s.vehicleInteractions)for(var action:vehicleActions.all())emitVehicleActionTo(s,action.owner(),vehicleActions.snapshot(action,now));}return;}
            if(!s.vehicleFleet&&(request.model!=0||vehicles.get(request.id)!=null&&vehicles.get(request.id).state.model!=0))throw new Vehicles.Rejected(2);
            if(!s.vehicleCapable||s.hp<=0||s.respawning||s.teleport!=null)throw new Vehicles.Rejected(2);
            if(request.op==VehicleWire.SEAT_HEARTBEAT){if(!vehicles.seatSupport(s))throw new Vehicles.Rejected(2);vehicles.heartbeat(s,request,now);return;}
            if(request.op==VehicleWire.CONTACT){
                if(!s.vehicleContacts||vehicles.driving(s)!=null&&!request.id.equals(VehicleWire.ZERO))throw new Vehicles.Rejected(2);
                VehicleWire contact=vehicleContacts.update(s,request,vehicles.get(request.id),s.pos,now);
                if(contact!=null)emitVehicleContact(s,contact);return;
            }
            Vehicles.Car<Session> car;
            if(request.op!=VehicleWire.STATE){if(now-s.vehicleCommandAt<150_000_000L)throw new Vehicles.Rejected(2);s.vehicleCommandAt=now;}
            switch(request.op){
                case VehicleWire.SPAWN -> {car=vehicles.spawn(s,request,s.pos,playerRules.entitled(character(s)),now);emitVehicle(car,VehicleWire.SNAPSHOT);}
                case VehicleWire.ENTER -> {
                    if(piloting(s))throw new Vehicles.Rejected(2);car=vehicles.get(request.id);double[] start=null;int seat=vehicles.seatSupport(s)&&request.sequence>0?request.sequence-1:0;if(car==null||!VehicleSeats.valid(car.state.model,seat))throw new Vehicles.Rejected(2);
                    if(car!=null&&s.vehicleInteractions&&VehicleActionProfiles.ENABLED[car.state.model]){
                        start=VehicleAction.local(car.state,s.pos);double[] door=VehicleSeats.DOOR[car.state.model][seat];
                        if(Math.hypot(start[0]-door[0],start[2]-door[2])>2.4||car.state.speed>1.5)throw new Vehicles.Rejected(2);start[1]+=.82;
                    }
                    car=vehicles.enter(s,request.id,seat,s.pos,now);detachVehicleContact(s);fpvPose(s);
                    if(start!=null){VehicleWire action=new VehicleWire();action.flags=VehicleAction.ENTER;emitVehicleAction(s,vehicleActions.begin(s,action,car,start,now));}
                    vehicleSeat(s,car.state);emitVehicle(car,VehicleWire.SNAPSHOT);
                }
                case VehicleWire.STATE -> {
                    var prior=vehicles.get(request.id);VehicleWire before=prior==null?null:prior.state;long updated=prior==null?now:prior.updated;
                    car=vehicles.state(s,request,now);
                    if(car!=null){for(Session occupant:car.occupants())vehicleSeat(occupant,car.state);emitVehicle(car,VehicleWire.SNAPSHOT);vehicleImpact(s,before,car.state,(now-updated)/1e9,now);}
                }
                case VehicleWire.EXIT -> {if(vehicleActions.busy(s,now))throw new Vehicles.Rejected(2);car=vehicles.get(request.id);int seat=Vehicles.seat(car,s);if(seat<0)throw new Vehicles.Rejected(2);double[] resolvedExit=VehicleAction.exitFeet(car.state,request);request.x=resolvedExit[0];request.y=resolvedExit[1];request.z=resolvedExit[2];var playing=vehicleActions.get(s);if(playing!=null&&playing.state().flags==VehicleAction.EXIT){double[] end=VehicleAction.local(car.state,new double[]{request.x,request.y+.82,request.z});playing.state().vx=(float)end[0];playing.state().vy=(float)end[1];playing.state().vz=(float)end[2];emitVehicleAction(s,vehicleActions.snapshot(playing,now));}double[] velocity=VehicleAction.exitVelocity(car.state,seat);car=vehicles.exit(s,request);emitVehicleSeatClear(s);s.pos=new double[]{request.x,request.y,request.z};s.velocity=velocity;s.motion.reset();s.speed=Math.sqrt(velocity[0]*velocity[0]+velocity[1]*velocity[1]+velocity[2]*velocity[2]);s.ground=car.state.speed<1.5;s.posture.set(0,0);emitVehicle(car,VehicleWire.SNAPSHOT);clearVehicleAction(s);fpvPose(s);}
                case VehicleWire.ACTION -> {
                    if(Boolean.getBoolean("sz.vehicle.actions.audit"))System.out.println("Vehicle action request kind="+request.flags+" supported="+s.vehicleInteractions+" busy="+vehicleActions.busy(s,now)+" prior="+(vehicleActions.get(s)==null?0:vehicleActions.get(s).state().flags));
                    var drone=drones.owned(s);if(!s.vehicleInteractions||drone!=null&&drone.piloting()||request.flags==VehicleAction.ENTER)throw new Vehicles.Rejected(2);
                    car=vehicles.get(request.id);if(request.flags==0){if(Vehicles.seat(car,s)<0)throw new Vehicles.Rejected(2);emitVehicleAction(s,vehicleActions.cancelExit(s,now));}else emitVehicleAction(s,vehicleActions.begin(s,request,car,car==null?null:VehicleSeats.seat(car.state.model,Math.max(0,Vehicles.seat(car,s))),now));
                }
                case VehicleWire.REMOVE -> {car=vehicles.remove(s,request.id);emitVehicle(car,VehicleWire.DESPAWN);}
                default -> throw new Vehicles.Rejected(2);
            }
        }catch(IllegalArgumentException error){VehicleWire denied=new VehicleWire();denied.op=VehicleWire.DENIED;denied.id=request==null?VehicleWire.ZERO:request.id;denied.sequence=error instanceof Vehicles.Rejected r?r.code:2;enqueueOrRetire(s,vehicleFrame(denied));}
    }
    private void vehicleImpact(Session driver,VehicleWire before,VehicleWire after,double seconds,long now){
        if(!VehicleImpacts.moving(before,after,seconds))return;
        Set<Session> riders=Collections.newSetFromMap(new IdentityHashMap<>());
        for(var link:vehicleContacts.all())if(link.state().id.equals(after.id)&&now-link.updated()<1_500_000_000L)riders.add(link.owner());
        for(Session victim:new ArrayList<>(sessions.values())){
            if(driver.gameplayMode!=PlayerRules.Mode.STALKER||victim==driver&&vehicles.driving(driver)!=null||!victim.ready||victim.hp<=0||victim.respawning||victim.teleport!=null||victim.gameplayMode!=PlayerRules.Mode.STALKER||vehicles.driving(victim)!=null||riders.contains(victim))continue;
            var hit=VehicleImpacts.hit(before,after,seconds,victim.pos,victim.velocity,CharacterScale.height(victim.posture.locomotion(),victim.characterScale),CharacterScale.radius(victim.characterScale));if(hit==null)continue;
            detachVehicleContact(victim);victim.velocity=hit.velocity();
            damage(driver,victim,victim.hp,new double[]{after.x,after.y,after.z},hit.point(),hit.impulse(),false);
        }
        for(var clone:clones.all())if(clone.health>0){var hit=VehicleImpacts.hit(before,after,seconds,clone.state.position,new double[3],CharacterScale.height(clone.state.locomotion,clone.state.scale),CharacterScale.radius(clone.state.scale));if(hit!=null)damageClone(driver,driver.grant.name(),clone,clone.health,hit.point(),hit.impulse(),false,hit.velocity());}
        if(vehicleBotImpacts==null)vehicleBotImpacts=new HashMap<>();
        vehicleBotImpacts.entrySet().removeIf(e->e.getValue().expires()<now);
        for(var victim:world.snapshot().entities()){
            if(victim.health()<=0||victim.traversal()!=null&&victim.traversal().seat()!=null&&vehicles.get(victim.traversal().seat().object())!=null)continue;
            double reach=scvehicles.protocol.VehicleTypes.LENGTH[after.model]+scvehicles.protocol.VehicleTypes.WIDTH[after.model]+seconds*55+16;if(Math.abs(victim.x()-after.x)>reach||Math.abs(victim.y()-after.y)>reach||Math.abs(victim.z()-after.z)>reach)continue;
            BotKey key=botKey(victim);if(vehicleBotImpacts.containsKey(key))continue;
            var bounds=victim.body();double radius=Math.max(.15,Math.max(bounds.xx()-bounds.x(),bounds.zz()-bounds.z())*.5);double[] foot={(bounds.x()+bounds.xx())*.5,bounds.y(),(bounds.z()+bounds.zz())*.5};
            var hit=VehicleImpacts.hit(before,after,seconds,foot,new double[]{victim.vx(),victim.vy(),victim.vz()},bounds.yy()-bounds.y(),radius);if(hit==null)continue;
            vehicleBotImpacts.put(key,new VehicleImpacts.Pending(hit,now+500_000_000L));damageBot(driver,victim,Math.min(10000,victim.health()),false);
        }
    }
    private void vehicleSeat(Session s,VehicleWire car){
        var action=vehicleActions.get(s);int index=Math.max(0,Vehicles.seat(vehicles.get(car.id),s));double[] seat=action==null?VehicleSeats.seat(car.model,index):VehicleAction.anchor(action.state(),VehicleActions.progress(action,System.nanoTime()));
        double cg=scvehicles.protocol.VehicleTypes.CG[car.model],cy=Math.cos(car.yaw),sy=Math.sin(car.yaw),cp=Math.cos(car.pitch),sp=Math.sin(car.pitch),cr=Math.cos(car.roll),sr=Math.sin(car.roll);
        double lx=seat[0]*cr-(seat[1]-cg)*sr,ly=seat[0]*sr+(seat[1]-cg)*cr,lz=seat[2],ry=ly*cp-lz*sp,rz=ly*sp+lz*cp;
        // Ordinary actor replication carries feet; interaction anchors carry the pelvis.
        s.pos=new double[]{car.x+lx*cy+rz*sy,car.y+cg+ry-.82,car.z-lx*sy+rz*cy};s.velocity=new double[]{car.vx,car.vy,car.vz};s.speed=car.speed;s.ground=(car.flags&2)!=0;s.lastInput=System.nanoTime();s.stamina=1;s.posture.set(vehicleActions.windowView(s,System.nanoTime())?0:11,vehicleActions.windowView(s,System.nanoTime())?0:11);
    }
    private void emitVehicleTo(Session viewer,Vehicles.Car<Session> car,int op){
        if(car.state.model!=0&&!viewer.vehicleFleet)return;VehicleWire state=Vehicles.snapshot(car,op,car.driver==null?-1:viewer==car.driver?0:car.driver.entity,viewer.vehicleArticulation);enqueueOrRetire(viewer,vehicleFrame(state));
        if(op!=VehicleWire.DESPAWN){emitVehicleSeatsTo(viewer,car);if(viewer.vehicleInteractions)for(Session owner:car.occupants()){var playing=vehicleActions.get(owner);if(playing!=null)emitVehicleActionTo(viewer,owner,vehicleActions.snapshot(playing,System.nanoTime()));}}
    }
    private void emitVehicleSeatsTo(Session viewer,Vehicles.Car<Session> car){
        if(!viewer.ready||!vehicles.seatSupport(viewer))return;
        for(Session owner:car.occupants()){VehicleWire seat=new VehicleWire();seat.op=VehicleWire.SEAT_SNAPSHOT;seat.id=car.state.id;seat.model=car.state.model;seat.driver=viewer==owner?0:owner.entity;seat.steer=Vehicles.seat(car,owner);seat.sequence=vehicles.nextSeatSequence();enqueueOrRetire(viewer,vehicleFrame(seat));}
    }
    private void emitVehicleSeatClear(Session owner){
        if(owner.hp>0&&!owner.respawning){owner.posture.set(0,0);fpvPose(owner);}
        for(Session viewer:new ArrayList<>(sessions.values()))if(viewer.ready&&vehicles.seatSupport(viewer)){VehicleWire seat=new VehicleWire();seat.op=VehicleWire.SEAT_SNAPSHOT;seat.driver=viewer==owner?0:owner.entity;seat.sequence=vehicles.nextSeatSequence();enqueueOrRetire(viewer,vehicleFrame(seat));}
    }
    private void emitVehicle(Vehicles.Car<Session> car,int op){for(Session viewer:new ArrayList<>(sessions.values()))if(viewer.ready&&viewer.vehicleCapable)emitVehicleTo(viewer,car,op);}
    private void emitVehicleContactTo(Session viewer,Session owner,VehicleWire contact){
        if(!viewer.ready||!viewer.vehicleContacts)return;
        VehicleWire copy=contact.copy();copy.sequence=++vehicleContactSequence;copy.driver=viewer==owner?0:owner.entity;enqueueOrRetire(viewer,vehicleFrame(copy));
    }
    private void emitVehicleContact(Session owner,VehicleWire contact){for(Session viewer:new ArrayList<>(sessions.values()))emitVehicleContactTo(viewer,owner,contact);}
    private void detachVehicleContact(Session owner){VehicleWire contact=vehicleContacts.detach(owner);if(contact!=null)emitVehicleContact(owner,contact);}
    private void emitVehicleActionTo(Session viewer,Session owner,VehicleWire action){if(!viewer.ready||!viewer.vehicleInteractions)return;VehicleWire copy=action.copy();copy.driver=viewer==owner?0:owner.entity;enqueueOrRetire(viewer,vehicleFrame(copy));}
    private void emitVehicleAction(Session owner,VehicleWire action){for(Session viewer:new ArrayList<>(sessions.values()))emitVehicleActionTo(viewer,owner,action);}
    private void clearVehicleAction(Session owner){VehicleWire clear=vehicleActions.clear(owner);if(clear!=null)emitVehicleAction(owner,clear);}
    private void advanceVehicles(long now){
        for(var action:vehicleActions.all()){
            Session owner=action.owner();var car=vehicles.get(action.state().id);
            if(sessions.get(owner.address)!=owner||owner.hp<=0||owner.respawning||owner.teleport!=null||Vehicles.seat(car,owner)<0){clearVehicleAction(owner);continue;}
            // The pilot rechecks the current native capsule/floor before final release.
            // Missing completion cancels the exit; it never teleports through unknown scenery.
            if(action.state().flags==VehicleAction.EXIT&&(now-action.started())*1e-9>action.state().speed+2){emitVehicleAction(owner,vehicleActions.cancelExit(owner,now));}
        }
        for(var link:vehicleContacts.expired(now,l->vehicles.get(l.state().id)!=null&&l.owner().ready&&l.owner().hp>0&&!l.owner().respawning&&l.owner().teleport==null&&vehicles.driving(l.owner())==null)){
            VehicleWire detached=new VehicleWire();detached.op=VehicleWire.CONTACT_SNAPSHOT;detached.sequence=link.state().sequence;emitVehicleContact(link.owner(),detached);
        }
        for(var car:vehicles.expiredPassengers(now,owner->{emitVehicleSeatClear(owner);clearVehicleAction(owner);}))emitVehicle(car,VehicleWire.SNAPSHOT);
        for(var car:vehicles.expired(now,owner->{emitVehicleSeatClear(owner);clearVehicleAction(owner);}))emitVehicle(car,vehicles.get(car.state.id)==car?VehicleWire.SNAPSHOT:VehicleWire.DESPAWN);
        for(var car:vehicles.stalledCoasts(now))emitVehicle(car,VehicleWire.SNAPSHOT);
        for(var car:vehicles.all()){
            for(Session owner:car.occupants())if(owner.hp<=0||owner.respawning||owner.teleport!=null){vehicles.detach(owner);emitVehicleSeatClear(owner);clearVehicleAction(owner);emitVehicle(car,VehicleWire.SNAPSHOT);}else vehicleSeat(owner,car.state);
            // Parked passengers also need current membership and action clocks.
            if(!car.passengers.isEmpty()&&(car.driver==null||now-car.updated>250_000_000L))emitVehicle(car,VehicleWire.SNAPSHOT);
        }
    }
    private void fpv(Session s,byte[] body){
        FpvPackets.Request r=null;
        try{
            r=FpvPackets.request(body);long now=System.nanoTime();
            if(r.operation()==4){s.fpvCapable=true;return;}
            if(!s.fpvCapable)throw new IllegalArgumentException("fpv_capability_required");
            if(r.operation()==6){recallFpv(s);return;}
            if(r.operation()==10){toggleFpv(s);return;}
            if(s.hp<=0||s.respawning||s.teleport!=null)throw new IllegalArgumentException("drone_owner_not_ready");
            switch(r.operation()){
                // These remaining legacy entry points must not start competing server flight
                // or consume an item before production placement validation is ported.
                case 1,5 -> throw new IllegalArgumentException("fpv_server_flight_disabled");
                case 11 -> placeFpv(s,r);
                case 12 -> {
                    if(vehicles.driving(s)!=null)throw new IllegalArgumentException("exit_vehicle_first");
                    drones.require(s,r.id());ItemStack controller=inventory(s).at(new Inventory.Address(r.aggregate(),r.container(),r.slot()));
                    if(controller==null||controller.id()!=10501)throw new IllegalArgumentException("fpv_item_missing");
                    var drone=drones.piloting(s,r.id(),true);fpvPose(s);emitFpv(drone,"connected");
                }
                case 13 -> {var drone=drones.piloting(s,r.id(),false);fpvPose(s);emitFpv(drone,"detached");}
                case 9 -> {
                    var p=r.state().position();if(Math.abs(p.x())>30_000_000||Math.abs(p.y())>30_000_000||Math.abs(p.z())>30_000_000)throw new IllegalArgumentException("fpv_position_out_of_bounds");
                    var change=drones.submit(s,r.state(),now);if(change!=null){emitFpv(change.drone(),change.event());if(change.detonated())explodeFpv(change.drone());}
                }
                default -> throw new IllegalArgumentException("invalid_fpv_operation");
            }
        }catch(IOException|IllegalArgumentException error){denyFpv(s,FpvPackets.itemRequestIdentity(body),Objects.toString(error.getMessage(),"fpv_rejected"));}
    }
    private void denyFpv(Session s,UUID id,String error){fpvDenied++;enqueueOrRetire(s,GamePackets.chat(error));if(id!=null&&s.pos!=null){var denied=new FpvPackets.State(id,-1,new FpvPackets.Rotation(1,0,0,0),fpvPosition(s.pos),new FpvPackets.Vector(0,0,0),0,false,false,new FpvPackets.Motors(0,0,0,0),0);enqueueOrRetire(s,Wire.dynamic(FpvPackets.SNAPSHOT,FpvPackets.snapshot(denied,"denied",false)));}}
    private void placeFpv(Session s,FpvPackets.Request request){
        var active=drones.byId(request.id());if(active!=null){if(active.owner()!=s)throw new IllegalArgumentException("drone_not_owned");emitFpv(active,"placed");return;}
        if(s.fpvUsed.contains(request.id()))throw new IllegalArgumentException("fpv_drone_identity_already_used");if(s.fpvUsed.size()>=1024)throw new IllegalArgumentException("fpv_drone_identity_limit");
        var sameId=fpvPlacementIds.get(request.id());if(sameId!=null){if(sameId.owner!=s)throw new IllegalArgumentException("drone_not_owned");return;}
        UUID character=character(s);if(fpvPlacements.containsKey(character)||fpvPlacements.size()>=32||inventories.busy(character))throw new IllegalArgumentException("fpv_placement_busy");drones.checkDeploy(s,request.id());
        ItemStack item=inventory(s).at(new Inventory.Address(1,0,request.slot()));if(item==null||item.id()!=12447)throw new IllegalArgumentException("fpv_item_missing");
        var p=request.position();var geometry=fpvMap.placement(point(s.pos),new World.Vec(p.x(),p.y(),p.z()));var admission=fpvReceipts.prepare(character,request.id(),request.slot(),p,inventories.fpvPending(character));
        var pending=new FpvPlacementTask(s,character,request,geometry,admission);fpvPlacements.put(character,pending);fpvPlacementIds.put(request.id(),pending);
    }
    private void finishFpvPlacement(FpvPlacementTask task){fpvPlacements.remove(task.character,task);fpvPlacementIds.remove(task.request.id(),task);}
    private boolean fpvPlacementOwner(FpvPlacementTask task){Session s=task.owner;return sessions.get(s.address)==s&&s.ready&&s.hp>0&&!s.respawning&&s.teleport==null&&s.life==task.life;}
    private void advanceFpvPlacements(){
        for(var task:new ArrayList<>(fpvPlacements.values())){
            try{
                if(task.error!=null)throw new IllegalArgumentException(task.error);
                if(!task.debitStarted){
                    if(!fpvPlacementOwner(task))throw new IllegalArgumentException("drone_owner_not_ready");if(System.nanoTime()>task.deadline)throw new IllegalArgumentException("fpv_placement_timeout");
                    if(!task.geometry.isDone()||!task.admission.isDone())continue;
                    var geometry=task.geometry.join();var admission=task.admission.join();if(!Arrays.equals(task.operator,task.owner.pos)){task.operator=task.owner.pos.clone();var p=task.request.position();task.geometry=fpvMap.placement(point(task.operator),new World.Vec(p.x(),p.y(),p.z()));continue;}geometry.checkCurrent();drones.checkDeploy(task.owner,task.request.id());
                    task.debitStarted=true;inventories.spendFpv(admission,receipt->task.receipt=fpvReceipts.committed(receipt),error->task.error=error);continue;
                }
                if(task.receipt==null||!task.receipt.isDone())continue;task.receipt.join();finishFpvPlacement(task);
                if(fpvPlacementOwner(task)){Session owner=task.owner;owner.fpvUsed.add(task.request.id());owner.fpvOwnerSent=0;fpvPlaced++;var drone=drones.deploy(owner,task.request.id(),task.request.position(),false,System.nanoTime());try{inventorySnapshot(owner,0);}catch(IllegalStateException slow){remove(owner);}if(drones.owned(owner)==drone)emitFpv(drone,"placed");}
            }catch(CompletionException|IllegalArgumentException error){finishFpvPlacement(task);Throwable cause=error;while(cause instanceof CompletionException&&cause.getCause()!=null)cause=cause.getCause();denyFpv(task.owner,task.request.id(),Objects.toString(cause.getMessage(),"fpv_placement_failed"));}
        }
    }
    private static FpvPackets.Vector fpvPosition(double[] p){return new FpvPackets.Vector((float)p[0],(float)p[1],(float)p[2]);}
    private final FpvPlacementSettings fpvUseSettings;
    private void toggleFpv(Session s){
        if(!s.fpvCapable)throw new IllegalArgumentException("fpv_capability_required");
        if(fpvPlacements.containsKey(character(s)))throw new IllegalArgumentException("fpv_placement_busy");
        if(drones.owned(s)!=null){recallFpv(s);return;}
        ItemStack held=inventory(s).at(new Inventory.Address(0,s.handContainer,0));fpvUseSettings.requireToggleItem(playerRules.entitled(character(s)),held==null?null:held.id());
        if(s.hp<=0||s.respawning||s.teleport!=null)throw new IllegalArgumentException("drone_owner_not_ready");
        s.fpvOwnerSent=0;var drone=drones.deploy(s,UUID.randomUUID(),new FpvPackets.Vector((float)s.pos[0],(float)(s.pos[1]+1.1),(float)s.pos[2]),true,System.nanoTime());fpvPose(s);emitFpv(drone,null);
    }
    private void recallFpv(Session s){var change=drones.recall(s);if(change!=null)emitFpv(change.drone(),change.event());}
    private boolean fpvNear(Session s,FpvPackets.Vector p){double x=s.pos[0]-p.x(),y=s.pos[1]-p.y(),z=s.pos[2]-p.z();return x*x+y*y+z*z<=256d*256;}
    private void emitFpv(FpvDrones.Drone<Session> drone,String event){
        var state=drone.state();UUID id=state.id();Session owner=drone.owner();long now=System.nanoTime();boolean terminal="exploded".equals(event)||"recalled".equals(event);
        if(terminal)fpvPose(owner);
        String worldEvent=terminal||"landed".equals(event)?event:null;
        Set<Session> previous=droneObservers.getOrDefault(id,Set.of()),next=new HashSet<>();
        byte[] removed=Wire.dynamic(FpvPackets.SNAPSHOT,FpvPackets.snapshot(state,"recalled",true));
        for(Session viewer:new ArrayList<>(sessions.values()))if(viewer.ready&&viewer.fpvCapable){
            boolean visible=viewer==owner||fpvNear(viewer,state.position());
            if(visible){next.add(viewer);long last=viewer.fpvWorldSent.getOrDefault(id,0L);if(terminal||now-last>=35_000_000L){enqueueOrRetire(viewer,Wire.dynamic(FpvPackets.SNAPSHOT,FpvPackets.snapshot(state,worldEvent,true)));if(!terminal&&drones.owned(owner)!=drone)return;if(sessions.get(viewer.address)==viewer)viewer.fpvWorldSent.put(id,now);}}
            else if(previous.contains(viewer)){enqueueOrRetire(viewer,removed);viewer.fpvWorldSent.remove(id);}
        }
        if(terminal){droneObservers.remove(id);for(Session viewer:sessions.values())viewer.fpvWorldSent.remove(id);}else if(drones.owned(owner)==drone)droneObservers.put(id,next);
        if(sessions.get(owner.address)==owner&&owner.fpvCapable&&(event!=null||now-owner.fpvOwnerSent>=50_000_000L)){enqueueOrRetire(owner,Wire.dynamic(FpvPackets.SNAPSHOT,FpvPackets.snapshot(state,event,false)));owner.fpvOwnerSent=now;}
    }
    private void explodeFpv(FpvDrones.Drone<Session> drone){
        // submit() retires before this callback, so duplicate/stale/reentrant packets cannot detonate twice.
        Session owner=drone.owner();var position=drone.state().position();fpvExplosions++;
        double[] origin={position.x(),position.y(),position.z()};
        UUID event=UUID.randomUUID();
        List<GrenadeTarget> targets=new ArrayList<>();
        for(Session victim:sessions.values())if(victim.ready&&victim.hp>0)targets.add(new GrenadeTarget(victim,victim.life,null,null,victim.pos.clone(),victim.stats));
        for(var clone:clones.all())if(clone.health>0)targets.add(new GrenadeTarget(null,0,clone,null,clone.state.position.clone(),clone.state.stats,CharacterScale.height(clone.state.locomotion,clone.state.scale)));
        for(var bot:world.snapshot().entities())if(bot.health()>0)targets.add(new GrenadeTarget(null,0,null,bot,new double[]{bot.x(),bot.y(),bot.z()},bot.stats()));
        for(var victim:targets){float health=victim.player()!=null?victim.player().hp:victim.cloneTarget()!=null?victim.cloneTarget().health:victim.bot().health();float amount=FpvExplosion.damage(position,victim.position(),health,victim.stats());if(amount<=0)continue;
            double[] end={victim.position()[0],victim.position()[1]+.9,victim.position()[2]};
            visibleDamage(origin,end,()->grenadeTargetLive(victim),()->combatBarriers.submit(event,origin,end,contact->Math.max(.001,FpvExplosion.damage(position,grenadeAreaScenes.closestShieldPoint(contact,origin),100,Map.of())),()->grenadeTargetLive(victim),()->{
                if(victim.player()!=null)damage(owner,owner.grant.name(),victim.player(),amount,origin,null,new double[3],false);
                else if(victim.cloneTarget()!=null)damageClone(owner,victim.cloneTarget(),amount,origin,new double[3],false);
                else damageBot(owner,victim.bot(),amount,false);
            },contact->{}),0);
        }
        byte[] presentation=Wire.dynamic(FpvExplosion.PACKET,fpvExplosion.body(position));
        for(Session viewer:new ArrayList<>(sessions.values()))if(viewer.ready&&(viewer==owner||fpvNear(viewer,position)||drones.owned(viewer)!=null&&fpvDistance(drones.owned(viewer).state().position(),position)<=256d*256))enqueueOrRetire(viewer,presentation);
    }
    private boolean grenadeTargetLive(GrenadeTarget target){
        if(target.player()!=null){var p=target.player();return sessions.get(p.address)==p&&p.life==target.life()&&p.hp>0&&p.gameplayMode==PlayerRules.Mode.STALKER;}
        if(target.cloneTarget()!=null)return clones.live(target.cloneTarget())&&target.cloneTarget().health>0;
        return world.snapshot().entities().stream().anyMatch(b->botKey(b).equals(botKey(target.bot()))&&b.health()>0);
    }
    private void visibleDamage(double[] from,double[] to,java.util.function.BooleanSupplier valid,Runnable apply,int retry){visibleDamage(from,to,valid,apply,retry,System.nanoTime()+1_000_000_000L);}
    private void visibleDamage(double[] from,double[] to,java.util.function.BooleanSupplier valid,Runnable apply,int retry,long deadline){
        if(System.nanoTime()>deadline||!valid.getAsBoolean())return;
        fpvMap.nearestVisible(List.of(new szserver.world.MapObstructionAccess.Ray(point(from),point(to)))).whenComplete((result,error)->{
            if(!combatCompleted.offer(()->{
                if(System.nanoTime()>deadline||!valid.getAsBoolean())return;
                if(error!=null){visibilityErrors++;return;}
                try{result.authorization().checkCurrent();}catch(IllegalArgumentException stale){if(retry<2)visibleDamage(from,to,valid,apply,retry+1,deadline);else visibilityErrors++;return;}
                if(result.index()<0){blockedHits++;return;}apply.run();
            }))combatCompletionDrops.incrementAndGet();
        });
    }
    private boolean hordeVictimLive(World.View actor,HordeCombat.Target target,Session victim){
        if(zombies.target()==null||!zombies.target().equals(target.id())||sessions.get(victim.address)!=victim||victim.life!=target.life()||victim.hp<=0||victim.respawning||victim.teleport!=null||victim.gameplayMode!=PlayerRules.Mode.STALKER)return false;
        var current=world.snapshot().entities().stream().filter(a->botKey(a).equals(botKey(actor))&&a.health()>0).findFirst().orElse(null);
        return current!=null&&HordeCombat.reaches(current,new HordeCombat.Target(target.id(),point(victim.pos),victim.posture.locomotion(),victim.life));
    }
    private void advanceHordeCombat(long now){
        var targets=sessions.values().stream().filter(p->p.ready&&p.hp>0&&!p.respawning&&p.teleport==null&&p.gameplayMode==PlayerRules.Mode.STALKER).map(p->new HordeCombat.Target(character(p),point(p.pos),p.posture.locomotion(),p.life)).toList();
        for(var strike:hordeCombat.advance(zombies.target(),world.snapshot().entities(),targets,now)){
            var victim=sessions.values().stream().filter(p->p.ready&&character(p).equals(strike.target().id())).findFirst().orElse(null);if(victim==null)continue;
            var actor=strike.actor();double[] from={actor.x(),actor.y()+1,actor.z()},to=CharacterScale.closestPoint(from,victim.pos,victim.posture.locomotion(),victim.characterScale);
            java.util.function.BooleanSupplier valid=()->hordeVictimLive(actor,strike.target(),victim);
            UUID request=UUID.nameUUIDFromBytes((actor.id()+":"+actor.incarnation()+":"+strike.sequence()).getBytes(java.nio.charset.StandardCharsets.UTF_8));
            visibleDamage(from,to,valid,()->combatBarriers.submit(request,from,to,Math.max(.001f,HordeCombat.ATTACK.damage(actor.stats(),Map.of())),valid,()->damage(null,"Зомби",victim,HordeCombat.ATTACK.damage(actor.stats(),victim.stats),from,to,new double[3],false),contact->{}),0);
        }
    }
    private boolean grenadeUserAllowed(Session s){return s.ready&&s.hp>0&&!s.respawning&&s.teleport==null&&!piloting(s)&&s.gameplayMode==PlayerRules.Mode.STALKER;}
    private boolean grenadeUseValid(Session s,GrenadeUse.Held held){return grenadeUserAllowed(s)&&held.valid(inventories.inventory(character(s)).at(new Inventory.Address(0,4,0)),s.life,s.handContainer,s.holstered);}
    private void grenadeStart(Session s,int mode){
        // OVERLAP_GRENADE is a native contextual override slot, not equipment4.
        if(mode==2||!grenadeUserAllowed(s)||inventories.busy(character(s))||grenadeBridge.busy(character(s))||thrownGrenades.size()>=szserver.world.Grenades.LIMIT||mode==0&&(s.handContainer!=4||s.holstered)){grenadeDenied++;return;}
        ItemStack item=inventories.inventory(character(s)).at(new Inventory.Address(0,4,0));GrenadeUse.Rules rules=item==null?null:grenadeUseRules.get(item.id());
        if(rules==null||!rules.handUse()||!s.grenadeUse.start(item,rules,mode,s.life,s.handContainer,s.holstered,System.nanoTime())){grenadeDenied++;return;}
    }
    private void grenadeThrow(Session s,GrenadePackets.Throw request){
        GrenadeUse.Held held=s.grenadeUse.held();
        if(held==null||!grenadeUseValid(s,held)||request.prediction()!=-1&&request.prediction()<=s.grenadePrediction){s.grenadeUse.cancel();grenadeDenied++;return;}
        long now=System.nanoTime();int elapsed=held.elapsed(now);
        if(request.charge()<1||request.charge()>held.rules().throwTicks()||elapsed<Math.max(0,held.rules().prepareTicks()-2)){s.grenadeUse.cancel();grenadeDenied++;grenadePrepareDenied++;return;}
        s.grenadeUse.take();boolean inHand=held.rules().expired(elapsed);
        if(request.prediction()!=-1)s.grenadePrediction=request.prediction();
        submitGrenade(s,held,request,elapsed,inHand);
    }
    private void advanceGrenadeUses(long now){
        for(Session s:List.copyOf(sessions.values())){GrenadeUse.Held held=s.grenadeUse.held();if(held==null)continue;
            if(!grenadeUseValid(s,held)){s.grenadeUse.cancel();grenadeUseCanceled++;continue;}
            int elapsed=held.elapsed(now);if(held.rules().expired(elapsed)&&!inventories.busy(character(s))&&!grenadeBridge.busy(character(s))){s.grenadeUse.take();submitGrenade(s,held,new GrenadePackets.Throw(0,-1,s.yaw,s.pitch),elapsed,true);}
        }
    }
    private void submitGrenade(Session s,GrenadeUse.Held held,GrenadePackets.Throw request,int elapsed,boolean inHand){
        var profile=grenadeProfiles.get(held.item().id());World.Vec origin=GrenadePackets.origin(CharacterScale.eye(s.pos,s.posture.locomotion(),s.characterScale),inHand?s.yaw:request.yaw(),inHand?s.pitch:request.pitch());
        var base=profile.flight();var flight=new szserver.world.Grenades.Flight(inHand?1:held.rules().remaining(profile.lifetime(),elapsed),base.gravity(),base.restitution(),base.friction(),base.drag());
        var operation=new ThrownGrenade(s,character(s),s.grant.name(),areaActor(s),inHand?-1:request.prediction(),profile);
        grenadeBridge.throwItem(character(s),s.grant.name(),new Inventory.Address(0,4,0),flight,origin,inHand?new World.Vec(0,0,0):profile.velocity(request),inHand,operation.context,id->{publishGrenadeLaunch(id,operation,held.item().id(),origin,inHand?new World.Vec(0,0,0):profile.velocity(request));grenadeThrows++;if(inHand)grenadeHeldExplosions++;if(sessions.get(s.address)==s){inventorySnapshot(s,0);selectHand(s,new HandPackets.Selection(s.handContainer,s.holstered));}},error->{grenadeDenied++;if(sessions.get(s.address)==s){inventorySnapshot(s,0);enqueueOrRetire(s,GamePackets.chat(error));}});
    }
    /** Spawn at the muzzle immediately after activation; waiting for the first saved flight batch hides short flights. */
    private void publishGrenadeLaunch(UUID id,ThrownGrenade state,int item,World.Vec origin,World.Vec velocity){
        thrownGrenades.put(id,state);state.entity=entityIds.actor();
        for(Session viewer:List.copyOf(sessions.values()))if(viewer.ready){double dx=viewer.pos[0]-origin.x(),dz=viewer.pos[2]-origin.z();if(dx*dx+dz*dz>128*128)continue;
            Recipient key=new Recipient(viewer.address,viewer.incarnation);state.recipients.add(key);
            byte[] prediction=viewer==state.owner?GrenadePackets.prediction(state.entity,state.prediction):new byte[0];
            enqueueOrRetire(viewer,Wire.concat(GrenadePackets.spawn(state.entity,item,origin,state.profile,velocity),prediction,GrenadePackets.update(state.entity,origin,velocity,tick,item)));
            // Accepted durable shot must not wait for the next 20 Hz replication flush.
            if(sessions.get(viewer.address)==viewer)try{flush(viewer,System.nanoTime());}catch(IOException failed){remove(viewer);}
        }
    }
    private void pollGrenades(){
        if(grenadeAck!=null){if(!grenadeAck.isDone())return;try{grenadeAck.join();}catch(CompletionException error){grenadePollErrors++;}grenadeAck=null;}
        if(grenadePoll!=null||grenadeSave!=null||thrownGrenades.isEmpty())return;
        List<GrenadeTarget> targets=new ArrayList<>();for(Session p:sessions.values())if(p.ready&&p.hp>0)targets.add(new GrenadeTarget(p,p.life,null,null,p.pos.clone(),p.stats,CharacterScale.height(p.posture.locomotion(),p.characterScale)));
        for(var clone:clones.all())if(clone.health>0)targets.add(new GrenadeTarget(null,0,clone,null,clone.state.position.clone(),clone.state.stats,CharacterScale.height(clone.state.locomotion,clone.state.scale)));
        for(var bot:world.snapshot().entities())if(bot.health()>0)targets.add(new GrenadeTarget(null,0,null,bot,new double[]{bot.x(),bot.y(),bot.z()},bot.stats(),Math.max(.3,bot.body().yy()-bot.body().y())));
        grenadePoll=world.submit(w->{List<GrenadeBlast> blasts=new ArrayList<>();for(var event:w.grenades().explosions().stream().limit(4).toList()){List<GrenadeTarget> visible=new ArrayList<>();for(var target:targets)if(grenadeDamage(event.profile(),event.position(),target.position(),target.stats())>0)visible.add(target);blasts.add(new GrenadeBlast(event,List.copyOf(visible)));}return new GrenadeBatch(w.grenades().views(),List.copyOf(blasts),w.grenades().saved());}).thenCompose(this::grenadeVisibility);
    }
    private CompletableFuture<GrenadeBatch> grenadeVisibility(GrenadeBatch batch){
        List<szserver.world.MapObstructionAccess.Ray> rays=new ArrayList<>();
        for(var blast:batch.blasts())for(var target:blast.visible())rays.add(new szserver.world.MapObstructionAccess.Ray(blast.event().position(),new World.Vec(target.position()[0],target.position()[1]+Math.min(.9,target.height()*.5),target.position()[2])));
        if(rays.isEmpty())return CompletableFuture.completedFuture(batch);
        return fpvMap.visibleRays(rays).thenApply(result->{List<GrenadeBlast> filtered=new ArrayList<>();int index=0;
            for(var blast:batch.blasts()){List<GrenadeTarget> clear=new ArrayList<>();for(var target:blast.visible())if(result.clear().get(index++))clear.add(target);filtered.add(new GrenadeBlast(blast.event(),List.copyOf(clear)));}
            return new GrenadeBatch(batch.views(),List.copyOf(filtered),batch.saved(),result.authorization());});
    }
    private void advanceGrenades(){
        if(grenadeSave==null){
            if(grenadePoll==null||!grenadePoll.isDone())return;var future=grenadePoll;grenadePoll=null;
            try{grenadeDelivery=future.join();if(grenadeDelivery.geometry()!=null)grenadeDelivery.geometry().checkCurrent();}catch(CompletionException|IllegalArgumentException failure){grenadePollErrors++;return;}
            long wall=System.currentTimeMillis();pendingPulseSources=samplePulseTargets(grenadeDelivery,wall);Map<UUID,Set<UUID>> targets=new HashMap<>();Map<UUID,Map<UUID,UUID>> revives=new HashMap<>();pendingPulseSources.forEach((id,source)->{if(source.pulseTargets()!=null)targets.put(id,source.pulseTargets());if(source.reviveTargets()!=null)revives.put(id,source.reviveTargets());});grenadeSave=grenadeCompletion.save(grenadeDelivery.saved(),grenadeDelivery.blasts().stream().map(b->b.event().id()).toList(),wall,targets,revives);
        }
        if(!grenadeSave.isDone())return;
        try{grenadeSave.join();}catch(CompletionException failure){grenadePollErrors++;throw new IllegalStateException("grenade_outbox_save_failed",failure);}
        GrenadeBatch batch=grenadeDelivery;grenadeSave=null;grenadeDelivery=null;pendingPulseSources=Map.of();
        for(var view:batch.views()){
            if(!view.stage().equals("flying"))continue;ThrownGrenade state=thrownGrenades.get(view.id());if(state==null)continue;if(state.entity==0)state.entity=entityIds.actor();
            for(Session viewer:List.copyOf(sessions.values()))if(viewer.ready){Recipient key=new Recipient(viewer.address,viewer.incarnation);double dx=viewer.pos[0]-view.position().x(),dz=viewer.pos[2]-view.position().z();boolean nearby=dx*dx+dz*dz<=128*128;
                if(nearby){if(state.recipients.add(key)){enqueueOrRetire(viewer,GrenadePackets.spawn(state.entity,view.item(),view.position(),state.profile,view.velocity()));if(viewer==state.owner)enqueueOrRetire(viewer,GrenadePackets.prediction(state.entity,state.prediction));}if(sessions.get(viewer.address)==viewer)enqueueOrRetire(viewer,GrenadePackets.update(state.entity,view.position(),view.velocity(),tick,view.item()));}
                else if(state.recipients.remove(key))enqueueOrRetire(viewer,GrenadePackets.remove(state.entity));
            }
        }
        for(var blast:batch.blasts()){
            var event=blast.event();ThrownGrenade state=thrownGrenades.remove(event.id());if(state==null)continue;grenadeExplosions++;
            double[] origin={event.position().x(),event.position().y(),event.position().z()};
            for(var target:blast.visible()){
                if(target.player()!=null&&!grenadeSelected(state,event.profile().item(),target.player()))continue;
                float amount=grenadeDamage(event.profile(),event.position(),target.position(),target.stats());if(amount<=0)continue;
                double[] end={target.position()[0],target.position()[1]+.9,target.position()[2]};
                combatBarriers.submit(event.id(),origin,end,contact->{double[] closest=grenadeAreaScenes.closestShieldPoint(contact,origin);closest[1]-=.9;return Math.max(.001,grenadeDamage(event.profile(),event.position(),closest,Map.of()));},
                    ()->grenadeTargetLive(target),()->{
                        if(target.player()!=null){damage(grenadeAttacker(state),state.name,target.player(),amount,origin,null,new double[3],false);grenadeHits++;}
                        else if(target.cloneTarget()!=null){damageClone(grenadeAttacker(state),state.name,target.cloneTarget(),amount,origin,new double[3],false);grenadeHits++;}
                        else {damageBot(grenadeAttacker(state),target.bot(),amount,false);grenadeHits++;}
                    },contact->{});
            }
            byte[] fx=Wire.dynamic("1155277618",event.profile().presentation(origin[0],origin[1],origin[2]));
            for(Session viewer:List.copyOf(sessions.values()))if(viewer.ready){Recipient key=new Recipient(viewer.address,viewer.incarnation);boolean observed=state.recipients.contains(key);double dx=viewer.pos[0]-origin[0],dz=viewer.pos[2]-origin[2];if(observed||viewer==state.owner||dx*dx+dz*dz<=256*256){if(observed&&state.entity!=0)enqueueOrRetire(viewer,GrenadePackets.remove(state.entity));if(sessions.get(viewer.address)==viewer)enqueueOrRetire(viewer,fx);}}
            if(state.entity!=0)entityIds.release(state.entity);
        }
        if(!batch.blasts().isEmpty())grenadeAck=world.submit(w->{for(var blast:batch.blasts())w.grenades().acknowledge(blast.event().id());return true;});
    }
    private Map<UUID,GrenadeAreaStore.Activation> samplePulseTargets(GrenadeBatch batch,long wall){
        Map<UUID,GrenadeReceipt> receipts=new HashMap<>();grenadeOutbox.states().forEach(state->receipts.put(state.receipt().projectile(),state.receipt()));Map<UUID,GrenadeAreaStore.Activation> out=new HashMap<>();
        for(var blast:batch.blasts()){var receipt=receipts.get(blast.event().id());if(receipt==null)continue;boolean pulse=GrenadeAreaScenes.pulse(receipt.areas()),revive=GrenadeAreaScenes.supportUltimate(receipt.areas());if(!pulse&&!revive)continue;var source=new GrenadeAreaStore.Activation(receipt.projectile(),receipt.character(),receipt.name(),receipt.item(),blast.event().position(),wall,receipt.areas(),receipt.ownerContext());Set<UUID> targets=new HashSet<>();Map<UUID,UUID> deaths=new HashMap<>();
            for(Session target:sessions.values())if(target.ready&&!target.respawning&&target.teleport==null&&target.gameplayMode==PlayerRules.Mode.STALKER&&!AreaExposure.select(grenadeAreaScenes.pulseContacts(List.of(source),target.pos,CharacterScale.height(target.posture.locomotion(),target.characterScale),wall),areaRelations,areaActor(target),this::liveAreaOwner,AreaRelations.Mode.DEFAULT).isEmpty()){
                if(pulse&&target.hp>0)targets.add(character(target));if(revive&&target.hp==0&&target.grenadeDeath!=null)deaths.put(character(target),target.grenadeDeath);
            }
            out.put(source.projectile(),new GrenadeAreaStore.Activation(source.projectile(),source.character(),source.owner(),source.item(),source.position(),source.started(),source.profiles(),source.ownerContext(),pulse?targets:null,revive?deaths:null));
        }return Map.copyOf(out);
    }
    private record PulseScope(Set<UUID> live,Set<UUID> eligible){}
    private PulseScope pulseScope(UUID target){Map<UUID,GrenadeAreaStore.Activation> sources=new HashMap<>(grenadeOutbox.pulses());sources.putAll(pendingPulseSources);grenadeAreas.states().forEach(source->sources.put(source.projectile(),source));Set<UUID> eligible=new HashSet<>();sources.values().stream().filter(source->source.pulseTargets()!=null&&source.pulseTargets().contains(target)).forEach(source->eligible.add(source.projectile()));return new PulseScope(Set.copyOf(sources.keySet()),Set.copyOf(eligible));}
    private void advanceGrenadeAreas(){
        combatBarriers.advance();advanceShieldShots();long now=System.currentTimeMillis(),monotonic=System.nanoTime();
        // Short native pulse scenes last only 101 ms: draining a published activation or a
        // completed formatting future must not wait for another 50 ms publication slot.
        boolean fresh=grenadeAreas.states().stream().filter(a->grenadeAreaScenes.visible(a.item())).flatMap(a->GrenadeAreaComponents.all(a).stream()).anyMatch(a->a.active(now)&&!areaScenes.containsKey(a.identity()));
        boolean prepared=areaScenes.values().stream().anyMatch(a->a.prepared!=null&&a.prepared.isDone());
        if(monotonic>=areaSceneAt||fresh||prepared){areaSceneAt=monotonic+50_000_000L;publishGrenadeAreas(now);}
        // Admit durable pulse work as soon as activation is published, including between game ticks.
        // Waiting only for effectTick can consume the entire native 101 ms thunder lifetime.
        if(grenadeAreas.states().stream().anyMatch(a->a.pulseTargets()!=null&&now<a.until()))for(Session target:List.copyOf(sessions.values()))if(target.ready)applyPulseEffects(target,now);
        advanceRevives(now);
        if(grenadeAreaExpiry!=null){if(!grenadeAreaExpiry.isDone())return;try{grenadeAreaExpiry.join();}catch(CompletionException error){throw new IllegalStateException("grenade_area_expiry_failed",error);}grenadeAreaExpiry=null;}
        if(now<grenadeAreaExpireAt||grenadeAreas.states().isEmpty())return;
        grenadeAreaExpireAt=now+500;grenadeAreaExpiry=grenadeAreas.expire(now);
    }
    private void publishGrenadeAreas(long now){
        Set<UUID> live=new HashSet<>();for(var activation:grenadeAreas.states())for(var component:GrenadeAreaComponents.all(activation))if(grenadeAreaScenes.visible(activation.item())&&component.active(now)){
            live.add(component.identity());AreaScene state=areaScenes.get(component.identity());if(state==null){if(nextAreaHandle>=-1)throw new IllegalStateException("grenade_area_scene_id_exhausted");state=new AreaScene(activation,component.index(),nextAreaHandle++);areaScenes.put(component.identity(),state);}
            float fraction=component.profile().shield()>0?grenadeAreas.shieldRemaining(activation.projectile())/component.profile().shield():1;
            if(fraction<=0)continue;
            if(state.prepared==null&&(state.scene==null||state.fraction!=fraction)){state.fraction=fraction;state.prepared=grenadeAreaScenes.prepare(state.activation,state.index,state.handle,fraction);}
            if(state.prepared!=null&&state.prepared.isDone())try{var updated=state.prepared.join();state.prepared=null;if(state.scene!=null){for(Session viewer:List.copyOf(sessions.values()))if(viewer.ready&&state.recipients.containsKey(new Recipient(viewer.address,viewer.incarnation))){enqueueOrRetire(viewer,GrenadeAreaPackets.remove(state.handle));areaSceneRemovals++;}state.recipients.clear();}state.scene=updated;}catch(CompletionException error){if(error.getCause() instanceof RejectedExecutionException){state.prepared=null;state.fraction=Float.NaN;}else throw new IllegalStateException("grenade_area_scene_prepare_failed",error);}

        }
        for(var entry:new ArrayList<>(areaScenes.entrySet()))if(!live.contains(entry.getKey())){
            AreaScene state=entry.getValue();for(Session viewer:List.copyOf(sessions.values()))if(viewer.ready&&state.recipients.containsKey(new Recipient(viewer.address,viewer.incarnation))){enqueueOrRetire(viewer,GrenadeAreaPackets.remove(state.handle));areaSceneRemovals++;}
            if(state.prepared!=null)state.prepared.cancel(false);areaScenes.remove(entry.getKey());
        }
        int budget=32;for(AreaScene state:areaScenes.values())if(state.scene!=null)for(Session viewer:List.copyOf(sessions.values()))if(viewer.ready&&viewer.teleport==null){
            Recipient recipient=new Recipient(viewer.address,viewer.incarnation);double dx=viewer.pos[0]-state.activation.position().x(),dz=viewer.pos[2]-state.activation.position().z();
            int owner=areaOwnerEntity(state.activation.character(),viewer);Integer sentOwner=state.recipients.get(recipient);if((sentOwner==null||state.scene.targeted()&&sentOwner!=owner)&&dx*dx+dz*dz<=256*256&&viewer.lanes[0].queued.size()<128){if(budget--==0)return;if(sentOwner!=null){enqueueOrRetire(viewer,GrenadeAreaPackets.remove(state.handle));areaSceneRemovals++;}state.recipients.put(recipient,owner);enqueueOrRetire(viewer,GrenadeAreaScenes.spawnFor(state.scene,owner));areaSceneSpawns++;}
        }
    }
    private AreaRelations.Actor areaActor(Session s){return areaRelations.actor(character(s),s.faction);}
    private AreaRelations.Actor liveAreaOwner(UUID id){for(Session s:sessions.values())if(s.ready&&id.equals(character(s)))return areaActor(s);return null;}
    private int areaOwnerEntity(UUID id,Session viewer){for(Session s:sessions.values())if(s.ready&&id.equals(character(s)))return s==viewer?0:s.entity;return -1;}
    private boolean grenadeSelected(ThrownGrenade state,int item,Session target){var areas=enabledAreaProfiles.get(item);if(areas==null||areas.isEmpty())return true;var owner=liveAreaOwner(state.character);return areaRelations.selected(areas.get(0).target(),owner==null?state.context:owner,areaActor(target),AreaRelations.Mode.DEFAULT);}
    private float grenadeDamage(ExplosiveProfile profile,World.Vec origin,double[] feet,Map<String,Float> stats){var rules=blastRules.get(profile.item());if(rules==null||rules.playerReduction()!=0)return GrenadePackets.damage(profile,origin,feet,stats);return GrenadePackets.damageChannels(profile,rules,origin,feet,stats).total();}
    private Session grenadeAttacker(ThrownGrenade state){for(Session player:sessions.values())if(player.ready&&state.character.equals(character(player))&&player.gameplayMode==PlayerRules.Mode.STALKER)return player;return null;}
    private static double fpvDistance(FpvPackets.Vector a,FpvPackets.Vector b){double x=(double)a.x()-b.x(),y=(double)a.y()-b.y(),z=(double)a.z()-b.z();return x*x+y*y+z*z;}
    private void nativeBlockBatch(Session s,NativeBlockPackets.Batch batch){
        if(s.teleport!=null||s.blockSaves.size()>=8){nativeBlockDenied++;for(var delta:batch.deltas())if(NativeBlockPackets.reachable(s.pos,delta.position()))repairNativeBlock(s,delta.position());return;}
        int proposed=batch.deltas().size();List<World.Block> rejectedPoints=batch.deltas().stream().map(NativeBlockPackets.Delta::position).filter(p->NativeBlockPackets.reachable(s.pos,p)).toList();s.blockSaves.addLast(nativeBlocks.mutate(character(s),batch,s.pos).handle((result,error)->error==null?result:new NativeBlockAccess.Outcome(0,0,proposed,rejectedPoints)));
    }
    private void nativeDestroy(Session s,byte[] body)throws IOException{World.Block p=NativeBlockPackets.target(body,true);if(s.teleport!=null||s.respawning||s.blockSaves.size()>=8){nativeBlockDenied++;if(NativeBlockPackets.reachable(s.pos,p))repairNativeBlock(s,p);return;}boolean reachable=NativeBlockPackets.reachable(s.pos,p);s.blockSaves.addLast(nativeBlocks.destroy(character(s),p,s.pos).handle((result,error)->error==null?result:new NativeBlockAccess.Outcome(0,0,1,reachable?List.of(p):List.of())));}
    private boolean streamNativeRepair(Session s){
        while(!s.blockSaves.isEmpty()&&s.blockSaves.peekFirst().isDone()){try{var result=s.blockSaves.removeFirst().join();nativeBlockDenied+=result.rejected();for(var p:result.repairs())repairNativeBlock(s,p);}catch(CompletionException error){nativeBlockFailed++;}}
        if(s.blockRepairs.isEmpty())return true;Lane lane=s.lanes[1];if(lane.queued.size()>32||lane.sent.size()>32)return false;
        var entry=s.blockRepairs.entrySet().iterator().next();long key=entry.getKey();BlockRepair repair=entry.getValue();int x=(int)(key>>32),z=(int)key;
        if(Math.abs((long)x-(int)Math.floor(s.pos[0]/16))>8||Math.abs((long)z-(int)Math.floor(s.pos[2]/16))>8){s.blockRepairs.remove(key);return true;}
        if(repair.future==null){repair.future=nativeBlocks.repair(new ArrayList<>(repair.points));return false;}
        if(!repair.future.isDone())return false;NativeBlockAccess.Repair ready;try{ready=repair.future.join();}catch(CompletionException error){nativeBlockFailed++;s.blockRepairs.remove(key);return true;}
        if(ready.chunk()==null){
            if(s.chunkRevisions.getOrDefault(key,0L)>ready.revision()){repair.future=null;return false;}int fragments=0;for(byte[] frame:ready.blocks())fragments+=(frame.length+FRAGMENT-1)/FRAGMENT;if(lane.queued.size()+fragments>MAX_QUEUE)return false;
            for(byte[] frame:ready.blocks())enqueue(s,1,frame);if(s.deltaChunk==key)s.deltas=null;s.blockRepairs.remove(key);nativeBlockRepairs++;return false;
        }
        if(ready.chunk().revision()!=terrainProjection.version(x,z)){repair.future=null;return false;}
        int fragments=(ready.chunk().frame().length+FRAGMENT-1)/FRAGMENT;for(byte[] frame:ready.blocks())fragments+=(frame.length+FRAGMENT-1)/FRAGMENT;
        if(lane.queued.size()+fragments>MAX_QUEUE)return false;for(byte[] frame:ready.blocks())enqueue(s,1,frame);enqueue(s,1,ready.chunk().frame());s.chunkRevisions.put(key,ready.chunk().revision());s.chunks.add(key);s.window.delivered(key);s.pendingChunks.remove(key);s.cachedRequests.remove(key);s.deliveries.record(key,ready.chunk().nativeRevision(),ready.chunk().revision(),System.nanoTime());if(s.deltaChunk==key)s.deltas=null;
        s.completeTiles=ready.chunk().completeFrameCount()>0?ready.chunk():null;s.completeTileAt=0;streamCompleteTiles(s);s.blockRepairs.remove(key);nativeBlockRepairs++;return false;
    }
    private void beginWorldJoin(Session s,LocalAccounts.Grant grant,UUID protocol){
        if(sessions.get(s.address)!=s)return;
        if(s.authenticationOrder==0){s.authenticationOrder=Math.incrementExact(authenticationOrder);authenticationOrder=s.authenticationOrder;}
        pruneCharacterSessions(s.authenticationOrder);
        if(!characterSessions.accept(grant.characterId(),s.authenticationOrder,s)){remove(s);return;}
        s.grant=grant;s.locationProtocol=protocol;
        // Ownership is settled before either account or inventory preparation can complete.
        for(Session other:new ArrayList<>(sessions.values()))if(other!=s&&other.grant!=null&&other.grant.characterId().equals(grant.characterId()))remove(other);
        s.accountPreparation=accounts.prepareCharacterCheck(grant.characterId());
    }
    private void pruneCharacterSessions(long candidateOrder){long minimum=candidateOrder;for(Session pending:sessions.values())if(pending.authentication!=null&&pending.authenticationOrder>0)minimum=Math.min(minimum,pending.authenticationOrder);characterSessions.prune(minimum);}
    private void beginInventoryWorldJoin(Session s){
        if(sessions.get(s.address)!=s)return;
        UUID id=s.grant.characterId();if(!inventories.admissionAllowed(id))throw new IllegalArgumentException("character_deleted_or_deleting");
        inventories.activate(id);s.inventoryPinned=true;s.inventoryPreparation=inventories.ensureDefaults(id);s.locationLease=locations.prepare(id);
    }
    private void beginPreparedWorldJoin(Session s,LocalAccounts.Grant grant,UUID protocol){
        if(sessions.get(s.address)!=s)return;
        if(!inventories.admissionAllowed(grant.characterId()))throw new IllegalArgumentException("character_deleted_or_deleting");
        s.grant=grant;s.restoredState=locations.state(grant.characterId());if(s.restoredState!=null){s.yaw=s.restoredState.yaw();s.pitch=s.restoredState.pitch();s.handContainer=s.restoredState.hand();s.holstered=s.restoredState.holstered();s.damageWall=s.restoredState.damagedAt();if(s.damageWall>0)s.lastDamage=System.nanoTime()-Math.min(3_600_000L,Math.max(0,System.currentTimeMillis()-s.damageWall))*1_000_000L;}s.gameplayMode=playerRules.initial(grant.characterId());s.faction=inventories.faction(grant.characterId());s.entity=entityIds.actor();s.pos=spawn(s);beginPlacement(s,placementCandidates(s,true));s.firearm=UUID.randomUUID();s.magazines=new byte[31][];for(int n=0;n<=30;n++){s.magazines[n]=baseMagazines[n].clone();ByteBuffer state=ByteBuffer.wrap(s.magazines[n]);int offset=s.magazines[n].length-18;state.putLong(offset,s.firearm.getMostSignificantBits()).putLong(offset+8,s.firearm.getLeastSignificantBits());}
            s.effects=CharacterEffects.read(inventories.gameplay(grant.characterId()),medicines,System.currentTimeMillis());inventories.prepare(grant.characterId(),new ItemStack(20004,1,0,s.magazines[30]),new ItemStack(12714,90,0,new byte[0]),()->refreshWeapon(s,()->{if(sessions.get(s.address)==s){s.inventoryReady=true;completeJoin(s);}}),error->{enqueue(s,0,GamePackets.chat(error));remove(s);});
    }
    private void sceneConfiguration(Session s,byte[] body)throws IOException{
        LootboxPackets.Open opening=LootboxPackets.request(body);if(opening!=null){openLootboxes(s,opening);return;}
        SceneConfigurationPackets.Request request=SceneConfigurationPackets.request(body);if(request==null)return;if(s.sceneReads.containsKey(request.sequence()))return;if(s.sceneReads.size()>=8){sceneDenied++;enqueue(s,0,GamePackets.chat("scene_configuration_busy"));return;}s.sceneReads.put(request.sequence(),sceneAccess.configuration(request,new ArrayList<>(s.chunks)));
    }
    private void dropItem(Session s,WorldItemPackets.Drop request){
        itemRequests++;int sequence=request.sequence();Runnable ack=()->enqueue(s,0,InventoryPackets.ack(0,sequence));
        if(sequence!=65535){if(s.dropSequence!=-1&&((sequence-s.dropSequence)&65535)>=32768||sequence==s.dropSequence){ack.run();return;}s.dropSequence=sequence;}
        if(s.hp<=0||s.respawning||s.teleport!=null||System.nanoTime()-s.lastItemDrop<100_000_000L){lastItemFailure="drop_not_ready:"+s.hp+":"+s.respawning+":"+(s.teleport!=null);inventorySnapshot(s,0);ack.run();return;}
        s.lastItemDrop=System.nanoTime();Inventory.Address address=request.address()==null?new Inventory.Address(0,s.handContainer,0):request.address();
        if(address.aggregate()>1||address.aggregate()==1&&address.container()!=0){inventorySnapshot(s,0);ack.run();return;}
        double[] position=CharacterScale.eye(s.pos,s.posture.locomotion(),s.characterScale);double eye=position[1]-s.pos[1];position[1]-=.3f;
        var velocity=szserver.world.WorldItems.throwVelocity(s.yaw,s.pitch,eye,random);
        inventories.dropWorldItem(character(s),address,itemWorld,position,new double[]{velocity.x(),velocity.y(),velocity.z()},plan->{itemDropped++;if(sessions.get(s.address)==s)inventoryChanged(s,plan,ack);},error->{lastItemFailure=error;itemFailures++;if(sessions.get(s.address)==s){inventorySnapshot(s,0);ack.run();enqueue(s,0,GamePackets.chat("Не удалось выбросить предмет: "+error));}});
    }
    private void advanceWorldItems(){
        worldItems.sync(inventories.worldItems());for(var gone:worldItems.removed()){for(Session s:List.copyOf(sessions.values()))if(s.worldItemsSeen.remove(gone.item.id()))enqueueOrRetire(s,WorldItemPackets.remove(gone.entity));entityIds.release(gone.entity);}
        long now=System.currentTimeMillis();
        if(tick%20==0){int left=2;for(var saved:inventories.worldItems().values())if(!saved.world().equals(itemWorld)&&now>=saved.expires()&&remoteItemExpiry.add(saved.id())){
            inventories.expireWorldItem(saved.id(),now).whenComplete((v,error)->{try{adminTask(()->{remoteItemExpiry.remove(saved.id());if(error!=null)itemFailures++;});}catch(RejectedExecutionException stopped){}});if(--left==0)break;
        }}
        List<Session> players=sessions.values().stream().filter(s->s.ready&&!s.respawning&&s.hp>0&&s.teleport==null).toList();
        worldItems.step(players.stream().map(s->point(s.pos)).toList());int settlements=2,expirations=2,pickups=4,pickupChecks=16;
        var bodies=new ArrayList<>(worldItems.all());
        for(int index=0;index<bodies.size();index++){var body=bodies.get((int)Math.floorMod((long)tick*17+index,bodies.size()));
            if(body.claiming||body.expiring)continue;
            if(now>=body.item.expires()){
                if(expirations-->0&&now>=body.retry){body.expiring=true;inventories.expireWorldItem(body.item.id(),now).whenComplete((v,error)->{if(error!=null)try{adminTask(()->{body.expiring=false;body.retry=System.currentTimeMillis()+1000;itemFailures++;});}catch(RejectedExecutionException stopped){}});}continue;
            }
            if(body.ground&&body.velocity.x()==0&&body.velocity.y()==0&&body.velocity.z()==0&&!body.settledSaved&&!body.settlePending&&settlements-->0&&now>=body.retry){
                body.settlePending=true;var p=body.position;inventories.settleWorldItem(body.item.id(),p.x(),p.y(),p.z()).whenComplete((v,error)->{try{adminTask(()->{body.settlePending=false;if(error==null)body.settledSaved=true;else{body.retry=System.currentTimeMillis()+1000;itemFailures++;}});}catch(RejectedExecutionException stopped){}});
            }
            if(pickups<=0||pickupChecks<=0||now-body.item.created()<2000||now<body.retry)continue;
            for(int pi=0;pi<players.size()&&pickupChecks>0;pi++){Session player=players.get((int)Math.floorMod((long)tick+pi,players.size()));
                if(body.item.pickupOwner()!=null&&!body.item.pickupOwner().equals(player.grant.name()))continue;
                double[] p=player.pos;var at=body.position;double radius=Math.max(.6,.6*player.characterScale);
                if(Math.abs(at.x()-p[0])>radius||Math.abs(at.z()-p[2])>radius||at.y()<p[1]-.25||at.y()>p[1]+CharacterScale.height(player.posture.locomotion(),player.characterScale)+.25)continue;
                pickupChecks--;if(!inventories.canCollect(character(player),body.item.stack()))continue;
                body.claiming=true;pickups--;inventories.collectWorldItem(character(player),body.item.id(),itemWorld,plan->{itemCollected++;if(sessions.get(player.address)==player)inventoryChanged(player,plan,()->{});},error->{body.claiming=false;body.retry=System.currentTimeMillis()+500;itemFailures++;});break;
            }
        }
        for(Session s:List.copyOf(sessions.values()))if(s.ready){
            Set<UUID> visible=new HashSet<>();for(var body:bodies)if(!body.expiring&&Math.abs(body.position.x()-s.pos[0])<64&&Math.abs(body.position.z()-s.pos[2])<64&&Math.abs(body.position.y()-s.pos[1])<48)visible.add(body.item.id());
            for(var it=s.worldItemsSeen.iterator();it.hasNext();){UUID id=it.next();if(!visible.contains(id)){for(var body:bodies)if(body.item.id().equals(id)){enqueueOrRetire(s,WorldItemPackets.remove(body.entity));break;}it.remove();}}
            int sent=0;for(int n=0;n<bodies.size()&&sent<32;n++){var body=bodies.get((int)Math.floorMod((long)tick*32+n,bodies.size()));if(!visible.contains(body.item.id()))continue;
                if(s.worldItemsSeen.add(body.item.id())){enqueueOrRetire(s,WorldItemPackets.spawn(body.entity,body.position,body.item.stack()));enqueueOrRetire(s,WorldItemPackets.update(body));sent++;}
                else if(body.velocity.x()!=0||body.velocity.y()!=0||body.velocity.z()!=0||tick%20==Math.floorMod(body.entity,20)){enqueueOrRetire(s,WorldItemPackets.update(body));sent++;}
            }
        }
    }

    private void lootboxPreview(Session s,int item){
        try{if(s.hp<=0||s.respawning)throw new IllegalArgumentException("lootbox_not_available");inventories.lootboxes().available(item);int count=LootboxOpening.count(inventory(s),item);if(count<1)throw new IllegalArgumentException("lootbox_missing");s.lootboxItem=item;boolean tradeable=inventories.lootboxes().tradeable(item);enqueue(s,0,LootboxPackets.preview(item,tradeable?0:count,tradeable?count:0,tradeable));}
        catch(IllegalArgumentException error){enqueue(s,0,GamePackets.chat(lootboxError(error.getMessage())));}
    }
    private static String lootboxError(String error){
        if(error==null)return "Не удалось открыть ящик.";
        return switch(error){case "inventory_full"->"Освободите место в рюкзаке: ящик и ключ сохранены.";case "lootbox_key_missing"->"Для открытия не хватает подходящих ключей.";case "lootbox_missing"->"В рюкзаке не хватает этих ящиков.";case "inventory_busy"->"Дождитесь завершения предыдущего действия.";default->"Не удалось открыть ящик: "+error;};
    }
    private void openLootboxes(Session s,LootboxPackets.Open request){
        LootReply existing=s.lootReplies.get(request.sequence());if(existing!=null){if(existing.request.equals(request)){if(existing.frame!=null)enqueue(s,0,existing.frame);}else enqueue(s,0,GamePackets.chat("lootbox_request_sequence_conflict"));return;}
        if(request.sequence()<=s.lootSequence){enqueue(s,0,LootboxPackets.denied(request.sequence()));return;}
        s.lootSequence=request.sequence();LootReply reply=new LootReply(request);s.lootReplies.put(request.sequence(),reply);while(s.lootReplies.size()>8)s.lootReplies.remove(s.lootReplies.keySet().iterator().next());
        java.util.function.Consumer<String> failed=error->{reply.frame=LootboxPackets.denied(request.sequence());if(sessions.get(s.address)==s){inventorySnapshot(s,0);enqueue(s,0,reply.frame);enqueue(s,0,GamePackets.chat(lootboxError(error)));}};
        if(s.hp<=0||s.respawning||s.lootboxItem!=request.item()){failed.accept("lootbox_not_available");return;}
        inventories.openLootboxes(character(s),request.item(),request.count(),result->{reply.frame=LootboxPackets.result(request.sequence(),result.receipt(),result.rewards());if(sessions.get(s.address)==s){inventoryChanged(s,result.plan(),()->{});enqueue(s,0,reply.frame);}},failed);
    }
    private boolean sceneWriteAllowed(Session s){
        if(!playerRules.editor(character(s),worldEditors)){sceneDenied++;enqueue(s,0,GamePackets.chat("scene_configuration_requires_world_editor"));return false;}advanceSceneConfiguration(s);if(s.sceneSaves.size()>=128){sceneDenied++;enqueue(s,0,GamePackets.chat("scene_write_queue_full"));return false;}return true;
    }
    private void anomaliesTick(){
        anomalyClock++;var targets=new ArrayList<AnomalySimulation.Target>();var players=new HashMap<String,Session>();var copies=new HashMap<String,PlayerClones.Clone<Session>>();var npcs=new HashMap<String,World.View>();var stasis=new HashMap<Session,UUID>();
        for(var s:sessions.values()){s.anomalySlow=1;if(s.anomalyMirrorUntil!=0&&(s.hp<=0||anomalyClock>=s.anomalyMirrorUntil)){s.anomalyMirrorUntil=0;refreshStats(s);}if(s.ready)players.put("player:"+character(s)+":"+s.life,s);if(s.ready&&s.hp>0&&!s.respawning&&s.teleport==null&&s.gameplayMode==PlayerRules.Mode.STALKER){String key="player:"+character(s)+":"+s.life;players.put(key,s);targets.add(new AnomalySimulation.Target(key,s.entity,point(s.pos),.3*s.characterScale,CharacterScale.height(s.posture.locomotion(),s.characterScale),s.hp,s.stats,false));}}
        for(var c:clones.all())if(c.health>0&&targets.size()<256){String key="clone:"+c.entity;copies.put(key,c);targets.add(new AnomalySimulation.Target(key,c.entity,point(c.state.position),.3*c.state.scale,CharacterScale.height(c.state.locomotion,c.state.scale),c.health,c.state.stats,false));}
        for(var n:world.snapshot().entities())if(n.health()>0&&targets.size()<256&&players.values().stream().anyMatch(p->Math.hypot(p.pos[0]-n.x(),p.pos[2]-n.z())<96)){String key="npc:"+n.id()+":"+n.incarnation();npcs.put(key,n);var b=n.body();targets.add(new AnomalySimulation.Target(key,botEntities.getOrDefault(botKey(n),-1),new World.Vec(n.x(),n.y(),n.z()),Math.max(b.xx()-b.x(),b.zz()-b.z())*.5,b.yy()-b.y(),n.health(),n.stats(),true));}
        if(anomalyQuery!=null&&anomalyQuery.isDone()){try{anomalyScene=anomalyQuery.join();anomalyScene.authorization().checkCurrent();anomalySceneAt=anomalyClock;}catch(RuntimeException error){anomalyScene=null;anomalyErrors++;}anomalyQuery=null;}
        if(anomalyScene!=null)try{anomalyScene.authorization().checkCurrent();if(anomalyClock-anomalySceneAt>20)anomalyScene=null;}catch(RuntimeException stale){anomalyScene=null;}
        if(anomalyQuery==null&&(anomalyScene==null||anomalyClock%4==0)&&!targets.isEmpty()){
            var areas=new LinkedHashMap<String,szserver.world.SceneCollision.Box>();for(var t:targets){var p=t.position();int x=(int)Math.floor(p.x()/8),y=(int)Math.floor(p.y()/8),z=(int)Math.floor(p.z()/8);areas.putIfAbsent(x+":"+y+":"+z,new szserver.world.SceneCollision.Box(x*8-4,y*8-4,z*8-4,x*8+12,y*8+12,z*8+12));}anomalyQuery=fpvMap.anomalies(new ArrayList<>(areas.values()));
        }
        var circuses=new HashSet<UUID>();if(anomalyScene!=null)for(var z:anomalyScene.zones())if(z.kind().equals("circus")){
            circuses.add(z.id());for(var viewer:sessions.values())if(viewer.ready&&Math.hypot(viewer.pos[0]-z.center().x(),viewer.pos[2]-z.center().z())<128&&viewer.anomalyCircuses.add(z.id()))for(int i=0;i<3;i++){
                int orbit=i+1;float phase=(float)((anomalyClock*2+i*120)%360),radius=(float)(z.radius()*.75);
                enqueue(viewer,0,Wire.dynamic("535532075",Wire.bytes(out->{out.writeLong(z.id().getMostSignificantBits());out.writeLong(z.id().getLeastSignificantBits());out.writeInt(orbit);})));
                enqueue(viewer,0,Wire.dynamic("1278965999",Wire.bytes(out->{out.writeLong(z.id().getMostSignificantBits());out.writeLong(z.id().getLeastSignificantBits());out.writeInt(orbit);out.writeFloat(phase);out.writeFloat(radius);out.writeFloat(2);})));}
        }
        for(var viewer:sessions.values())viewer.anomalyCircuses.retainAll(circuses);
        var events=anomalies.step(anomalyClock,anomalyScene==null?List.of():anomalyScene.zones(),targets);
        for(var e:events){Session victim=players.get(e.target().key());var clone=copies.get(e.target().key());var npc=npcs.get(e.target().key());var z=e.zone();
            switch(e.type()){
                case "damage"->{double[] source={z.center().x(),z.center().y(),z.center().z()};if(victim!=null)damage(null,z.asset(),victim,e.amount(),source,victim.pos,new double[3],false);else if(clone!=null)damageClone(null,z.asset(),clone,e.amount(),clone.state.position,new double[3],false);else if(npc!=null)damageBot(null,npc,e.amount(),false);}
                case "mirror"->{if(victim!=null){victim.anomalyMirrorUntil=anomalyClock+Math.round(e.amount());refreshStats(victim);}}
                case "slow"->{if(npc!=null&&anomalyClock%2==0)world.submit(w->w.anomalySlow(npc.id(),npc.incarnation(),e.amount()));if(victim!=null){victim.anomalySlow=Math.min(victim.anomalySlow,e.amount());stasis.merge(victim,z.id(),(a,b)->a.compareTo(b)<=0?a:b);}}
                case "velocity"->{if(npc!=null){double[] v=e.velocity();world.submit(w->w.impulse(npc.id(),npc.incarnation(),new World.Vec(v[0],v[1],v[2]),e.set()));}if(victim!=null)enqueue(victim,0,Wire.frame(1033,Wire.bytes(out->{double[] v=e.velocity();for(int i=0;i<3;i++)out.writeDouble(v[i]+(e.set()?0:victim.velocity[i]));out.writeBoolean(true);})));}
                case "activate"->{for(var viewer:sessions.values())if(viewer.ready&&Math.hypot(viewer.pos[0]-z.center().x(),viewer.pos[2]-z.center().z())<128){int entity=viewer==victim?0:e.target().entity();enqueue(viewer,0,Wire.dynamic("1726532531",Wire.bytes(out->{out.writeLong(z.id().getMostSignificantBits());out.writeLong(z.id().getLeastSignificantBits());out.writeInt(entity);})));}}
                case "enter","leave"->{for(var viewer:sessions.values())if(viewer.ready&&e.target().entity()>=0&&Math.hypot(viewer.pos[0]-z.center().x(),viewer.pos[2]-z.center().z())<128){int entity=viewer==victim?0:e.target().entity();enqueue(viewer,0,Wire.dynamic("1410763904",Wire.bytes(out->{out.writeLong(z.id().getMostSignificantBits());out.writeLong(z.id().getLeastSignificantBits());out.writeInt(entity);out.writeBoolean(e.type().equals("leave"));})));}}
            }
        }
        for(var s:sessions.values())if(s.ready){UUID next=stasis.get(s);if(!Objects.equals(next,s.anomalyStasis)){if(s.anomalyStasis!=null)stasisOverlay(s,s.anomalyStasis,false);if(next!=null)stasisOverlay(s,next,true);s.anomalyStasis=next;}}
    }
    private void stasisOverlay(Session s,UUID id,boolean active){enqueue(s,0,Wire.dynamic("1149446340",Wire.bytes(out->{out.writeLong(id.getMostSignificantBits());out.writeLong(id.getLeastSignificantBits());out.writeByte(active?0:1);})) );}
    // The native controller echoes applied impulses/capture phases. Neither echo grants damage or movement authority.
    private void anomalyVelocityEcho(byte[] body)throws IOException{if(body.length!=25)throw new IOException("anomaly_velocity_echo_size");var in=ByteBuffer.wrap(body);for(int i=0;i<3;i++)if(!Double.isFinite(in.getDouble()))throw new IOException("anomaly_velocity_echo_value");if((in.get()&255)>1)throw new IOException("anomaly_velocity_echo_flag");anomalyVelocityEchoes++;}
    private void anomalyForceEcho(byte[] body)throws IOException{if(body.length!=20)throw new IOException("anomaly_force_echo_size");int phase=ByteBuffer.wrap(body).getInt();if(phase<0||phase>100000)throw new IOException("anomaly_force_echo_phase");}
    private void anomalyStrengthSave(Session s,byte[] body)throws IOException{if(!sceneWriteAllowed(s))return;sceneWrite(s,sceneAccess.queuedAnomalyStrength(s.sceneWriter,szserver.client.AnomalySettings.strength(body),new ArrayList<>(s.chunks)));}
    private void anomalyConfigurationSave(Session s,byte[] body)throws IOException{
        if(!sceneWriteAllowed(s))return;sceneWrite(s,sceneAccess.queuedAnomaly(s.sceneWriter,szserver.client.AnomalySettings.save(body),new ArrayList<>(s.chunks)));
    }
    private void sceneConfigurationSave(Session s,byte[] body,boolean compressed)throws IOException{
        if(!sceneWriteAllowed(s))return;SceneConfigurationPackets.Save request=compressed?SceneConfigurationPackets.settings(body):SceneConfigurationPackets.save(body);sceneWrite(s,sceneAccess.queuedSave(s.sceneWriter,request,new ArrayList<>(s.chunks)));
    }
    private void sceneEdit(Session s,String kind,byte[] body){if(!sceneWriteAllowed(s))return;sceneWrite(s,sceneAccess.queuedEdit(s.sceneWriter,kind,body,new ArrayList<>(s.chunks)));}
    private void sceneWrite(Session s,CompletableFuture<JsonObject> future){
        if(future.isCompletedExceptionally()){try{future.join();}catch(CompletionException error){if(error.getCause() instanceof RejectedExecutionException){sceneDenied++;enqueue(s,0,GamePackets.chat("scene_write_queue_full"));return;}}}s.sceneSaves.addLast(future);
    }
    private void advanceSceneConfiguration(Session s){
        for(var iterator=s.sceneReads.entrySet().iterator();iterator.hasNext();){var entry=iterator.next();if(!entry.getValue().isDone())continue;iterator.remove();try{enqueue(s,0,entry.getValue().join());}catch(CompletionException error){sceneFailed++;enqueue(s,0,GamePackets.chat("scene_configuration_read_failed"));}}
        while(!s.sceneSaves.isEmpty()&&s.sceneSaves.peekFirst().isDone()){var completed=s.sceneSaves.removeFirst();try{JsonObject result=completed.join();if(result.get("status").getAsString().equals("committed"))enqueue(s,0,GamePackets.chat("Изменения объекта сохранены."));}catch(CompletionException error){sceneFailed++;enqueue(s,0,GamePackets.chat("scene_configuration_save_failed"));}}
    }
    private void join(Session s,UUID protocol){
        if(sessions.get(s.address)!=s)return;
        enqueue(s,0,Wire.concat(GamePackets.context(protocol),GamePackets.snapshot(),szserver.client.ChunkDistancePackets.frame(s.window.radius()),scaledReady(s),GamePackets.vitals(tick,0,s.hp,s.maxHp,s.animation.aiming())));
        enqueue(s,0,GamePackets.attributes(tick,0,equipment(s)));inventorySnapshot(s,0);syncHand(s);syncAmmo(s);
        var initialEnvironment=environment.state();enqueue(s,0,EnvironmentPackets.clock(environment.clockAnchor(),System.currentTimeMillis()));
        sendEffects(s,List.of(),s.effects.active(System.currentTimeMillis()));sendPulseEffects(s,List.of(),s.pulses.effects(System.currentTimeMillis()));sendSickness(s,true);
        sendPermissions(s,true);enqueue(s,0,GamePackets.attributes(++tick,0,Factions.attributes(s.faction)));s.ready=true;sendPulseEffects(s,List.of(),s.pulses.effects(System.currentTimeMillis()));resetPoses(s);for(Session viewer:sessions.values())if(viewer!=s&&viewer.ready&&viewer.scaleCapable)sendScale(viewer,s);for(Session other:sessions.values())if(other!=s&&other.ready){enqueue(other,0,Wire.concat(GamePackets.spawn(s.entity,s.grant.name(),s.pos,s.yaw,s.pitch),GamePackets.attributes(tick,s.entity,equipment(s)),GamePackets.attributes(tick,s.entity,hand(s)),GamePackets.attributes(tick,s.entity,Factions.attributes(s.faction)),GamePackets.vitals(tick,s.entity,s.hp,s.maxHp,s.animation.aiming())));enqueue(s,0,Wire.concat(GamePackets.spawn(other.entity,other.grant.name(),other.pos,other.yaw,other.pitch),GamePackets.attributes(tick,other.entity,equipment(other)),GamePackets.attributes(tick,other.entity,hand(other)),GamePackets.attributes(tick,other.entity,Factions.attributes(other.faction)),GamePackets.vitals(tick,other.entity,visibleHealth(other),other.maxHp,other.animation.aiming())));}
            chunkWindow(s);System.out.println("Native joined: "+s.grant.name()+" entity="+s.entity);
    }
    private UUID character(Session s){return s.grant.characterId();}
    private void meleeSwing(Session s,byte[] body)throws IOException{
        if(body.length!=8)throw new IOException("melee_swing_size");ByteBuffer raw=ByteBuffer.wrap(body);int kind=raw.getInt(),combo=raw.getInt();if(kind<0||kind>2||combo<0||combo>32)throw new IOException("melee_swing_kind");
        long now=System.nanoTime();ItemStack knife=inventory(s).at(new Inventory.Address(0,5,0));if(knife==null||s.hp<=0||s.holstered||s.physicalPending||s.unjamming!=null||s.unjammingPending||!s.weaponActions.isEmpty()||kind!=2&&s.handContainer!=5||s.reloadAt!=0||now+15_000_000L<s.nextAttack||now<s.drawUntil||inventories.busy(character(s)))return;
        MeleeProfile profile=MeleeProfile.from(inventories.catalog().get(knife.id()).properties(),kind,combo);long interval=(long)(Math.max(profile.delay(),profile.cooldown())*1e9);s.animation.swing(kind);s.nextAttack=now+interval;s.swing=new Swing(kind,combo,knife.id(),now+(long)(profile.delay()*1e9),now+interval+750_000_000,profile);
    }
    private void meleeHit(Session s,byte[] body)throws IOException{
        if(body.length<7||body.length>8192)throw new IOException("melee_hit_size");var in=Wire.input(body);int kind=in.readByte(),combo=in.readByte();in.readInt();int count=in.readUnsignedByte();if(count<1||count>16)throw new IOException("melee_targets_limit");Set<Integer> claims=new HashSet<>();
        for(int n=0;n<count;n++){if(in.readUnsignedByte()!=0)return;claims.add(in.readInt());for(int coordinate=0;coordinate<6;coordinate++)if(!Double.isFinite(in.readDouble()))throw new IOException("melee_geometry_nonfinite");if(!Float.isFinite(in.readFloat()))throw new IOException("melee_geometry_nonfinite");int absent=in.readUnsignedByte();if(absent>1)throw new IOException("melee_bone_presence");if(absent==0&&in.readUTF().length()>128)throw new IOException("melee_bone_limit");}if(in.available()!=0)throw new IOException("melee_trailing_bytes");
        Swing swing=s.swing;long now=System.nanoTime();ItemStack knife=inventory(s).at(new Inventory.Address(0,5,0));lastMelee=Map.of("stage","claim","kind",kind,"combo",combo,"targets",claims,"pending",swing!=null,"hand",s.handContainer,"untilReadyMs",swing==null?0:(swing.ready()-now)/1e6);if(swing==null||knife==null||knife.id()!=swing.weapon()||s.hp<=0||s.holstered||swing.kind()!=2&&s.handContainer!=5||kind!=swing.kind()||combo!=swing.combo()||now<swing.ready()-150_000_000||now>swing.expires())return;s.swing=null;
        double yaw=Math.toRadians(s.yaw),pitch=Math.toRadians(s.pitch);double[] eye=CharacterScale.eye(s.pos,s.posture.locomotion(),s.characterScale),forward={-Math.sin(yaw)*Math.cos(pitch),-Math.sin(pitch),Math.cos(yaw)*Math.cos(pitch)};
        List<MeleeCandidate> candidates=new ArrayList<>();long life=s.life;
        for(Session target:sessions.values())if(target!=s&&target.ready&&target.hp>0&&target.gameplayMode==PlayerRules.Mode.STALKER&&claims.contains(target.entity)){
            double[] point=CharacterScale.closestPoint(eye,target.pos,target.posture.locomotion(),target.characterScale);double d=PlayerPosture.meleeDistance(eye,point,forward,swing.profile());if(d<0)continue;long targetLife=target.life;float amount=swing.profile().damage(s.stats,target.stats);
            candidates.add(new MeleeCandidate(target.entity,d,point,amount,()->{if(s.gameplayMode==PlayerRules.Mode.STALKER&&sessions.get(target.address)==target&&target.life==targetLife&&target.hp>0)damage(s,target,amount);}));
        }
        for(var clone:clones.all())if(clone.health>0&&clone.recipients.contains(s)&&claims.contains(clone.entity)){
            double[] point=CharacterScale.closestPoint(eye,clone.state.position,clone.state.locomotion,clone.state.scale);double d=PlayerPosture.meleeDistance(eye,point,forward,swing.profile());if(d<0)continue;float amount=swing.profile().damage(s.stats,clone.state.stats);
            candidates.add(new MeleeCandidate(clone.entity,d,point,amount,()->damageClone(s,clone,amount,point,new double[3],false)));
        }
        for(World.View bot:world.snapshot().entities()){
            Integer entity=botEntities.get(botKey(bot));if(bot.health()<=0||entity==null||!s.bots.contains(botKey(bot))||!claims.contains(entity))continue;
            double[] point=PlayerPosture.meleePoint(eye,bot.collisionParts(),forward,swing.profile());if(point==null)continue;double d=PlayerPosture.meleeDistance(eye,point,forward,swing.profile());float amount=swing.profile().damage(s.stats,bot.stats());
            candidates.add(new MeleeCandidate(entity,d,point,amount,()->damageBot(s,bot,amount,false)));
        }
        candidates.sort(Comparator.comparingDouble(MeleeCandidate::distance));if(candidates.isEmpty())lastMelee=Map.of("stage","miss","kind",kind,"combo",combo,"pending",false);else visibleMelee(s,life,swing,eye,List.copyOf(candidates),now+1_000_000_000L,0);
    }
    private record MeleeCandidate(int entity,double distance,double[] point,float damage,Runnable hit){}
    private void visibleMelee(Session attacker,long life,Swing swing,double[] eye,List<MeleeCandidate> candidates,long deadline,int retries){
        var rays=candidates.stream().map(c->new szserver.world.MapObstructionAccess.Ray(new World.Vec(eye[0],eye[1],eye[2]),new World.Vec(c.point()[0],c.point()[1],c.point()[2]))).toList();
        fpvMap.nearestVisible(rays).whenComplete((result,error)->{
            if(!combatCompleted.offer(()->{
                if(sessions.get(attacker.address)!=attacker||attacker.life!=life||attacker.hp<=0||attacker.holstered||attacker.handContainer!=5&&swing.kind()!=2||System.nanoTime()>deadline)return;
                ItemStack knife=inventory(attacker).at(new Inventory.Address(0,5,0));if(knife==null||knife.id()!=swing.weapon())return;
                Throwable failure=error;while(failure instanceof CompletionException&&failure.getCause()!=null)failure=failure.getCause();
                if(failure==null)try{result.authorization().checkCurrent();}catch(IllegalArgumentException stale){failure=stale;}
                if(failure!=null){if("block_publication_stale".equals(failure.getMessage())&&retries<2){visibleMelee(attacker,life,swing,eye,candidates,deadline,retries+1);return;}visibilityErrors++;return;}
                if(result.index()<0){blockedHits++;lastMelee=Map.of("stage","blocked","kind",swing.kind(),"combo",swing.combo());return;}
                var candidate=candidates.get(result.index());lastMelee=Map.of("stage","hit","kind",swing.kind(),"combo",swing.combo(),"target",candidate.entity(),"distance",candidate.distance());combatBarriers.submit(UUID.nameUUIDFromBytes((character(attacker)+":"+life+":"+swing.ready()).getBytes(java.nio.charset.StandardCharsets.UTF_8)),eye,candidate.point(),Math.max(.001f,swing.profile().damage(attacker.stats,Map.of())),
                    ()->sessions.get(attacker.address)==attacker&&attacker.life==life&&attacker.hp>0&&System.nanoTime()<=deadline,candidate.hit(),contact->lastMelee=Map.of("stage","shield","target",candidate.entity()));
            }))combatCompletionDrops.incrementAndGet();
        });
    }
    private void damage(Session attacker,Session victim,float amount){damage(attacker,victim,amount,attacker.pos,null,new double[3],false);}
    private void damage(Session attacker,Session victim,float amount,double[] source,double[] impact,double[] impulse,boolean head){damage(attacker,attacker.grant.name(),victim,amount,source,impact,impulse,head);}
    private void damage(Session attacker,String killerName,Session victim,float amount,double[] source,double[] impact,double[] impulse,boolean head){
        if(attacker!=null&&attacker.gameplayMode!=PlayerRules.Mode.STALKER||victim.gameplayMode!=PlayerRules.Mode.STALKER||victim.hp<=0||!Float.isFinite(amount)||amount<=0)return;enqueueOrRetire(victim,GamePackets.receivedDamage(source));float applied=Math.min(victim.hp,amount);victim.hp=Math.max(0,victim.hp-amount);victim.lastDamage=System.nanoTime();victim.damageWall=System.currentTimeMillis();if(victim.hp==0)playerDeath(victim,attacker,killerName,impact,impulse);vitals(victim);
        if(attacker!=null&&sessions.get(attacker.address)==attacker)enqueueOrRetire(attacker,Wire.dynamic("207064166",Wire.bytes(o->{o.writeBoolean(false);o.writeInt(head?4:3);o.writeBoolean(true);o.writeFloat(applied);o.writeFloat(0);o.writeBoolean(false);o.writeBoolean(false);o.writeBoolean(victim.hp==0);o.writeBoolean(false);o.writeBoolean(false);o.writeFloat(0);o.writeFloat(0);o.writeInt(5);o.writeBoolean(false);o.writeBoolean(false);})));
    }
    private boolean vitalsPending(UUID id){return medicineCommits.containsKey(id)||deathCommits.containsKey(id)||areaHealCommits.containsKey(id)||pulseCommits.containsKey(id)||reviveCommits.containsKey(id)||reviveChecks.containsKey(id);}
    private void publishVitals(VitalsTask task,VitalsReceipt receipt,CharacterLocations.PlayerState state){task.save=locations.commitVitals(task.id,receipt,state);locations.releaseVitals(task.id);task.held=false;}
    private void completeVitals(VitalsTask task,Map<UUID,VitalsTask> tasks){if(task.held)locations.releaseVitals(task.id);tasks.remove(task.id,task);task.finished.run();}
    private void advanceVitals(){
        for(var tasks:List.of(medicineCommits,deathCommits,areaHealCommits,pulseCommits,reviveCommits))for(VitalsTask task:new ArrayList<>(tasks.values()))if(task.save!=null&&task.save.isDone()){
            try{task.save.join();vitalsCommits++;}catch(CompletionException error){vitalsCommitFailures++;if(!closed&&sessions.get(task.session.address)==task.session)remove(task.session);}
            completeVitals(task,tasks);
        }
    }
    private void clearDeathEffects(Session victim){
        var pulseScope=pulseScope(character(victim));var pulseBefore=victim.pulses.effects(Long.MIN_VALUE);victim.pulses=victim.pulses.onDeath(pulseScope.live(),pulseScope.eligible());if(!pulseBefore.isEmpty())sendPulseEffects(victim,pulseBefore,List.of());
        clearSickness(victim);var change=victim.effects.onDeath();victim.effects=change.after();refreshStats(victim);if(!change.removed().isEmpty())sendEffects(victim,change.removed(),List.of());
        UUID id=character(victim);if(deathCommits.containsKey(id))return;
        locations.holdVitals(id);VitalsTask task=new VitalsTask(id,victim,()->{});deathCommits.put(id,task);
        inventories.clearEffectsOnDeath(id,playerState(victim),receipt->{publishVitals(task,receipt,playerState(victim));},error->{vitalsCommitFailures++;completeVitals(task,deathCommits);},pulseScope.live(),pulseScope.eligible(),victim.grenadeDeath);
    }
    private void corpseDespawn(CorpseLifecycle.Corpse<Recipient> corpse){if(corpse==null)return;byte[] frame=Wire.frame(2028,Wire.bytes(out->out.writeInt(corpse.entity())));for(Recipient target:corpse.recipients()){Session viewer=sessions.get(target.address());if(viewer!=null&&viewer.ready&&viewer.incarnation.equals(target.incarnation()))enqueueOrRetire(viewer,frame);}}
    private void playerDeath(Session victim,Session killer,String killerName,double[] impact,double[] impulse){
        byte[] dyingPose=animation(victim);double[] dyingPosition=victim.pos.clone(),dyingVelocity=victim.velocity.clone();float dyingYaw=victim.yaw,dyingPitch=victim.pitch;
        deaths++;victim.grenadeDeath=UUID.randomUUID();victim.life++;victim.deathGeneration++;victim.deadUntil=System.nanoTime()+CorpseLifecycle.RESPAWN_NANOS;victim.damageWall=System.currentTimeMillis();victim.reloadAt=0;victim.swing=null;
        List<Session> recipients=sessions.values().stream().filter(viewer->viewer.ready).toList();var added=corpses.create(System.nanoTime(),recipients.stream().map(viewer->new Recipient(viewer.address,viewer.incarnation)).toList());corpseDespawn(added.evicted());var corpse=added.corpse();victim.lastCorpse=corpse;ItemStack armor=inventory(victim).at(new Inventory.Address(0,1,0));
        // All ready viewers currently own this source model; snapshot precedes its zero-HP delta.
        for(Session viewer:recipients){byte[] pose=viewer==victim?new byte[0]:GamePackets.movement(tick,victim.entity,dyingPosition,dyingYaw,dyingPitch,dyingPose);enqueueOrRetire(viewer,Wire.concat(pose,GamePackets.corpse(corpse.entity(),viewer==victim?0:victim.entity,victim.grant.name(),dyingPosition,dyingYaw,dyingVelocity,impact,impulse,armor,null,victim.characterScale)));}
        recallFpv(victim);victim.animation.reset();clearDeathEffects(victim);
        if(killer!=null&&sessions.get(killer.address)==killer)enqueueOrRetire(killer,GamePackets.killNotification(victim.grant.name()));
        if(killer!=null||!killerName.equals("Окружение")){byte[] feed=GamePackets.killFeed(victim.grant.name(),killerName);for(Session viewer:recipients)enqueueOrRetire(viewer,feed);}
        enqueueOrRetire(victim,Wire.concat(GamePackets.respawnOptions(),GamePackets.death(killerName,victim.pos)));enqueueOrRetire(victim,GamePackets.attributes(++tick,0,Map.of(201,new byte[]{0})));
    }
    private void requestRespawn(Session s,byte[] body)throws IOException{var in=Wire.input(body);if(!in.readUTF().equals("spawn")||in.available()!=0)throw new IOException("invalid_respawn_destination");if(s.hp>0||s.respawning)return;if(System.nanoTime()-s.deadUntil<0){respawnCooldowns++;return;}if(inventories.busy(character(s))||vitalsPending(character(s)))return;if(s.teleport!=null)s.teleport.respawnRequested=true;else beginRespawn(s);}
    private void damageBot(Session attacker,World.View victim,float amount,boolean head){
        if(!Float.isFinite(amount)||amount<=0)return;BotKey key=botKey(victim);Integer entity=botEntities.get(key);
        VehicleImpacts.Pending vehicleHit=vehicleBotImpacts==null?null:vehicleBotImpacts.get(key);
        long life=attacker==null?0:attacker.life;
        world.submit(owner->owner.damage(victim.id(),victim.incarnation(),amount)).whenComplete((result,error)->{
            if(!combatCompleted.offer(()->{
                if(vehicleHit!=null&&(error!=null||result==null||!result.killed()))vehicleBotImpacts.remove(key,vehicleHit);
                if(error!=null){botDamageErrors++;return;}if(result==null){blockedHits++;return;}if(result.applied()<=0)return;botHits++;if(result.killed()){botDeaths++;emitBotCorpse(victim,null,new double[3]);}
                for(Session viewer:sessions.values())if(entity!=null&&viewer.ready&&viewer.bots.contains(key))enqueue(viewer,0,szserver.client.NpcVisuals.vitals(tick,entity,result.remaining(),victim.maximumHealth(),victim.aiming(),victim));
                if(attacker!=null&&sessions.get(attacker.address)==attacker&&attacker.life==life)enqueueOrRetire(attacker,Wire.dynamic("207064166",Wire.bytes(o->{o.writeBoolean(false);o.writeInt(head?4:3);o.writeBoolean(true);o.writeFloat(result.applied());o.writeFloat(0);o.writeBoolean(false);o.writeBoolean(false);o.writeBoolean(result.killed());o.writeBoolean(false);o.writeBoolean(false);o.writeFloat(0);o.writeFloat(0);o.writeInt(5);o.writeBoolean(false);o.writeBoolean(false);})));
            }))combatCompletionDrops.incrementAndGet();
        });
    }
    private void emitBotCorpse(World.View victim,double[] impact,double[] impulse){
        BotKey key=botKey(victim);Integer entity=botEntities.get(key);if(entity==null||!emittedBotCorpses.add(key))return;
        World.View pose=livingBotPoses.getOrDefault(key,victim);float yaw=BotAnimation.heading(pose,botHeadings.getOrDefault(key,0f));
        List<Session> viewers=sessions.values().stream().filter(v->v.ready&&v.bots.contains(key)).toList();if(viewers.isEmpty())return;
        var added=corpses.create(System.nanoTime(),viewers.stream().map(v->new Recipient(v.address,v.incarnation)).toList());corpseDespawn(added.evicted());
        double[] position={pose.x(),pose.y(),pose.z()},velocity={pose.vx(),pose.vy(),pose.vz()};VehicleImpacts.Pending collision=vehicleBotImpacts==null?null:vehicleBotImpacts.remove(key);if(collision!=null&&collision.expires()>=System.nanoTime()){velocity=collision.hit().velocity();impact=collision.hit().point();impulse=collision.hit().impulse();}var profile=NpcVisuals.profile(pose.id());
        ItemStack armor=pose.presentation().roots().containsKey(1)?ItemStack.read(com.google.gson.JsonParser.parseString(pose.presentation().roots().get(1)).getAsJsonObject()):null;
        byte[] animation=PlayerAnimationPackets.component(BotAnimation.state(pose,yaw).swing(hordeCombat.swing(pose)));
        byte[] body=GamePackets.corpse(added.corpse().entity(),entity,pose.kind(),position,yaw,velocity,impact,impulse,armor,profile==null?null:profile.skin());
        for(Session viewer:viewers)enqueueOrRetire(viewer,Wire.concat(GamePackets.movement(tick,entity,position,yaw,0,animation),body));
    }
    private void simulationCorpses(){
        var snapshot=world.snapshot();Set<BotKey> present=new HashSet<>();for(var actor:snapshot.entities()){BotKey key=botKey(actor);present.add(key);if(actor.health()<=0)emitBotCorpse(actor,null,new double[3]);}
        livingBotPoses.keySet().retainAll(present);emittedBotCorpses.retainAll(present);if(vehicleBotImpacts!=null)vehicleBotImpacts.entrySet().removeIf(e->!present.contains(e.getKey())||e.getValue().expires()<System.nanoTime());
    }
    private void drainCombat(){for(int count=0;count<64;count++){Runnable completion=combatCompleted.poll();if(completion==null)return;try{completion.run();}catch(RuntimeException error){botDamageErrors++;}}}
    private Map<String,Float> effectStats(Session s,CharacterEffects effects,long now){return PlayerEffectStats.compose(inventories.stats(character(s)),java.util.stream.Stream.concat(effects.active(now).stream().map(e->e.definition(medicines).stats()),java.util.stream.Stream.concat(s.pulses.active(now).stream().map(e->e.definition().stats()),s.anomalyMirrorUntil>anomalyClock?java.util.stream.Stream.of(Map.of("heal_efficiency",-65f)):java.util.stream.Stream.empty())).toList(),s.selectedAreas.stream().map(c->c.profile().stats()).toList(),sicknessRules.stats(s.sickness.states()),sicknessRules.profiles().stream().map(SicknessRules.Profile::accumulation).collect(java.util.stream.Collectors.toSet()));}
    private void refreshStats(Session s){s.stats=effectStats(s,s.effects,System.currentTimeMillis());s.maxHp=HealthRules.maximum(s.stats);s.hp=Math.min(s.hp,s.maxHp);s.maxStamina=staminaRules.capacity(s.stats);s.stamina=Math.min(s.stamina,s.maxStamina);s.movementConfiguration=PlayerMovementConfiguration.project(packets.config(),s.stats,s.maxStamina);}
    private byte[] movementConfiguration(Session s){if(s.movementConfiguration==null)s.movementConfiguration=PlayerMovementConfiguration.project(packets.config(),s.stats,s.maxStamina);return PlayerMovementConfiguration.slowed(PlayerMovementConfiguration.withCharacterScale(s.movementConfiguration,s.characterScale),s.anomalySlow);}
    private void inputButtons(Session s,List<ClientPackets.Button> buttons){
        for(var button:buttons){if(button.bindings().contains(33)||button.device()==0&&button.code()==1){boolean aiming=button.pressed()&&!s.holstered&&s.hp>0&&(!piloting(s)||vehicleActions.windowFire(s,System.nanoTime()));s.animation.aim(aiming);enqueue(s,0,GamePackets.attributes(tick,0,Map.of(201,new byte[]{(byte)(aiming?1:0)})));for(Session other:sessions.values())if(other!=s&&other.ready)enqueue(other,0,GamePackets.attributes(tick,s.entity,Map.of(201,new byte[]{(byte)(aiming?1:0)})));}
            if(!button.pressed())continue;for(int[] choice:new int[][]{{17,6},{18,7},{19,5},{20,4},{21,10}})if(button.bindings().contains(choice[0])){
                selectHand(s,new szserver.client.HandPackets.Selection(choice[1],false));break;
            }
        }
    }
    private boolean administrator(Session s){return playerRules.entitled(character(s))&&s.gameplayMode==PlayerRules.Mode.ADMIN;}
    private void sendPermissions(Session s,boolean edge){boolean entitled=playerRules.entitled(character(s)),effective=administrator(s);if(edge&&effective)enqueue(s,0,Wire.concat(szserver.client.PermissionPackets.refresh(++tick,0,entitled,false),szserver.client.PermissionPackets.refresh(tick,1,entitled,false)));enqueue(s,0,Wire.concat(szserver.client.PermissionPackets.refresh(++tick,0,entitled,effective),szserver.client.PermissionPackets.refresh(tick,1,entitled,effective),szserver.client.PermissionPackets.capabilities(effective),GamePackets.developerPermissions(playerRules.editor(character(s),worldEditors))));}
    private void changeMode(Session s,byte[] body){try{PlayerRules.Mode selected=playerRules.requested(character(s),PlayerRules.decode(body));if(s.gameplayMode==selected)return;if(inventories.busy(character(s)))throw new IllegalArgumentException("gameplay_mode_busy");s.gameplayMode=selected;if(s.reloadAt!=0)enqueue(s,0,Wire.dynamic("365014982",new byte[0]));s.reloadAt=0;s.swing=null;s.animation.reset();sendPermissions(s,false);enqueue(s,0,GamePackets.attributes(++tick,0,Map.of(208,Wire.bytes(out->out.writeInt(0)),209,new byte[]{0},201,new byte[]{0})));loadout(s);}catch(IllegalArgumentException error){enqueue(s,0,GamePackets.chat(error.getMessage()));}}
    private void sendEffects(Session s,List<CharacterEffects.Effect> removed,Collection<CharacterEffects.Effect> added){enqueueOrRetire(s,GamePackets.effects(tick,removed,added,medicines,statOrdinals,System.currentTimeMillis()));}
    private void sendPulseEffects(Session s,List<CharacterEffects.Effect> removed,Collection<CharacterEffects.Effect> added){if(!s.ready)return;enqueueOrRetire(s,GamePackets.effects(++tick,removed,added,medicines,statOrdinals,System.currentTimeMillis(),instantEffectKind));}
    private void clearSickness(Session s){syncAreaEffects(s,List.of(),System.currentTimeMillis());s.selectedAreas=List.of();s.sickness=SicknessState.EMPTY;s.sicknessExposure=Map.of();sendSickness(s,false);}
    private void sendSickness(Session s,boolean force){
        if(!s.ready&&!force)return;var current=s.sickness.states();Set<String> kinds=new TreeSet<>(current.keySet());kinds.addAll(s.sicknessSent.keySet());for(var p:sicknessRules.profiles())if(force||s.sicknessExposure.containsKey(p.accumulation())||s.sicknessExposureSent.containsKey(p.accumulation()))kinds.add(p.kind());
        for(String kind:kinds){var p=sicknessRules.get(kind);var state=current.getOrDefault(kind,sicknessRules.empty(kind));float raw=Math.max(0,s.sicknessExposure.getOrDefault(p.accumulation(),0f));float filtered=p.protectedDose(raw,s.stats.getOrDefault(p.protection(),0f),1);enqueueOrRetire(s,Wire.dynamic(SicknessPackets.UPDATE,SicknessPackets.body(p,state,filtered,raw,raw>0?1:0)));sicknessUpdates++;}
        s.sicknessSent=current;s.sicknessExposureSent=s.sicknessExposure;s.sicknessSendAt=System.currentTimeMillis()+200;
    }
    private void syncAreaEffects(Session s,List<AreaExposure.Contact> contacts,long now){
        var change=s.areaEffects.update(contacts,now);if(!s.ready||!change.changed())return;
        for(int at=0;at<change.removed().size();at+=16)enqueueOrRetire(s,GamePackets.effects(++tick,change.removed().subList(at,Math.min(at+16,change.removed().size())),List.of(),medicines,statOrdinals,now,continuousEffectKind));
        for(int at=0;at<change.added().size();at+=16)enqueueOrRetire(s,GamePackets.effects(++tick,List.of(),change.added().subList(at,Math.min(at+16,change.added().size())),medicines,statOrdinals,now,continuousEffectKind));
    }
    private void advanceSickness(Session s,long wall){
        if(s.gameplayMode!=PlayerRules.Mode.STALKER){if(!s.sickness.states().isEmpty()||!s.sicknessExposure.isEmpty()){clearSickness(s);refreshStats(s);savePlayerState(s);}return;}
        var contacts=AreaExposure.select(grenadeAreaScenes.contacts(grenadeAreas.states(),s.pos,CharacterScale.height(s.posture.locomotion(),s.characterScale),wall),areaRelations,areaActor(s),this::liveAreaOwner,AreaRelations.Mode.DEFAULT);syncAreaEffects(s,contacts,wall);boolean contactsChanged=!contacts.equals(s.selectedAreas);s.selectedAreas=contacts;var zone=AreaExposure.bank(contacts);if(contactsChanged||!zone.equals(s.sicknessExposure)){float oldHp=s.hp,oldMax=s.maxHp;s.sicknessExposure=zone;refreshStats(s);if(s.hp!=oldHp||s.maxHp!=oldMax)vitals(s);}areaHealing(s,contacts,wall);Map<String,SicknessRules.State> after=new TreeMap<>();Map<String,Float> damage=new LinkedHashMap<>();boolean changed=false;
        for(var p:sicknessRules.profiles()){
            var before=s.sickness.states().getOrDefault(p.kind(),sicknessRules.empty(p.kind()));float dose=s.sicknessExposure.getOrDefault(p.accumulation(),0f);
            if(before.power()==0&&dose==0&&s.stats.getOrDefault(p.accumulation(),0f)<=0)continue;
            var step=sicknessRules.step(p.kind(),before,s.stats,dose);sicknessTicks++;if(step.after().power()>0)after.put(p.kind(),step.after());changed|=step.levelChanged();if(step.damage()>0)damage.put(p.kind(),step.damage());
        }
        var previous=s.sickness;var next=after.isEmpty()?SicknessState.EMPTY:new SicknessState(wall,after);s.sickness=next;if(changed)refreshStats(s);
        if(!next.equals(previous))savePlayerState(s);if((!after.equals(s.sicknessSent)||!s.sicknessExposure.equals(s.sicknessExposureSent))&&wall>=s.sicknessSendAt)sendSickness(s,false);
        if(sessions.get(s.address)!=s)return;
        for(var entry:damage.entrySet()){if(s.hp<=0)break;damage(null,entry.getKey(),s,entry.getValue(),s.pos,null,new double[3],false);sicknessHits++;}
    }
    private CharacterLocations.StateSave savePlayerState(Session s){
        UUID id=character(s);var state=playerState(s);
        if(locations.position(id)==null)locations.observe(id,point(s.pos));
        if(s.stateSave==null||s.stateSave.isDone()||!state.equals(s.lastStateRequest)){s.stateSave=locations.requestState(id,state);s.lastStateRequest=state;}
        return s.stateSave;
    }
    private void advancePlayerStates(){
        locations.advanceStateWrites();
        for(Session s:new ArrayList<>(sessions.values()))if(s.stateSave!=null&&s.stateSave.isDone()){
            try{s.stateSave.join();}catch(CompletionException failure){if(!closed)remove(s);continue;}
            if(s.vitalsQueued&&s.ready&&!s.respawning){s.vitalsQueued=false;var state=locations.durableState(character(s));if(state==null)continue;
                enqueueOrRetire(s,GamePackets.vitals(tick,0,state.health(),s.maxHp,s.animation.aiming()));
                for(Session other:new ArrayList<>(sessions.values()))if(other!=s&&other.ready)enqueueOrRetire(other,GamePackets.vitals(tick,s.entity,state.health(),s.maxHp,s.animation.aiming()));
            }
        }
    }
    private float visibleHealth(Session s){var state=locations.durableState(character(s));return state==null?s.hp:state.health();}
    private void vitals(Session s){if(s.ready&&s.pos!=null){s.vitalsQueued=true;savePlayerState(s);}}
    private void medicine(Session s,Integer slot){
        medicine(s,slot,()->{});
    }
    private void medicine(Session s,Integer slot,Runnable finished){
        UUID id=character(s);
        if(s.gameplayMode!=PlayerRules.Mode.STALKER||s.hp<=0||s.respawning||inventories.busy(id)||vitalsPending(id)){enqueue(s,0,GamePackets.chat("medicine_not_available"));finished.run();return;}
        long now=System.currentTimeMillis();var selection=s.effects.select(inventory(s),slot,medicines,now);if(selection==null){enqueue(s,0,GamePackets.chat("medicine_missing_or_cooldown"));inventorySnapshot(s,0);finished.run();return;}
        long medicineDeath=s.deathGeneration;CharacterEffects.Change change;try{change=s.effects.use(selection.medicine(),medicines,now);}catch(IllegalArgumentException|IllegalStateException error){enqueue(s,0,GamePackets.chat(error.getMessage()));inventorySnapshot(s,0);finished.run();return;}ItemStack original=inventory(s).at(selection.address());
        Map<String,Float> plannedStats=effectStats(s,change.after(),now);float instantHealing=selection.medicine().instant()*Math.max(0,1+plannedStats.getOrDefault("heal_efficiency",0f)/100);
        var expected=new CharacterLocations.PlayerState(s.yaw,s.pitch,HealthRules.instant(s.hp,plannedStats,selection.medicine().instant()),s.handContainer,s.holstered,s.damageWall,s.stamina,s.sickness);
        VitalsReceipt receipt=new VitalsReceipt(inventory(s).revision()+1,expected);JsonObject progress=change.after().json();progress.add(VitalsReceipt.KEY,receipt.json());
        locations.holdVitals(id);VitalsTask task=new VitalsTask(id,s,finished);medicineCommits.put(id,task);
        inventories.mutate(id,before->{if(!original.equals(before.at(selection.address())))throw new IllegalArgumentException("medicine_inventory_changed");Map<Inventory.Address,ItemStack> changes=new LinkedHashMap<>();changes.put(selection.address(),original.count(original.count()-1));return before.replace(changes,1);},progress,plan->{
            if(!inventories.admissionAllowed(id)){s.hp=expected.health();publishVitals(task,receipt,playerState(s));return;}
            if(s.deathGeneration!=medicineDeath||s.hp<=0){clearDeathEffects(s);inventoryChanged(s,plan,()->{});completeVitals(task,medicineCommits);return;}
            var previous=s.effects.active(Long.MIN_VALUE);s.effects=CharacterEffects.read(inventories.gameplay(id),medicines,System.currentTimeMillis());refreshStats(s);s.hp=Math.min(s.maxHp,Math.max(0,s.hp+instantHealing));
            // The locations marker precedes later HP snapshots; startup can replay the inventory receipt.
            publishVitals(task,receipt,playerState(s));
            if(!closed&&sessions.get(s.address)==s){inventoryChanged(s,plan,()->{});var current=s.effects.active(Long.MIN_VALUE);Set<Integer> oldIds=new HashSet<>(),newIds=new HashSet<>();previous.forEach(e->oldIds.add(e.id()));current.forEach(e->newIds.add(e.id()));sendEffects(s,previous.stream().filter(e->!newIds.contains(e.id())).toList(),current.stream().filter(e->!oldIds.contains(e.id())).toList());vitals(s);}
        },error->{completeVitals(task,medicineCommits);if(!closed&&sessions.get(s.address)==s){enqueue(s,0,GamePackets.chat(error));inventorySnapshot(s,0);}});
    }
    private void areaHealing(Session s,List<AreaExposure.Contact> contacts,long now){
        UUID id=character(s);if(s.hp<=0||s.respawning||s.teleport!=null||s.gameplayMode!=PlayerRules.Mode.STALKER||inventories.busy(id)||vitalsPending(id))return;
        for(var contact:contacts){var source=contact.activation();int profile=source.profiles().indexOf(contact.profile());if(contact.profile().heal()<=0||profile<0||inventories.areaApplied(id,source.projectile(),profile))continue;
            float healing=contact.profile().heal()*Math.max(0,1+s.stats.getOrDefault("heal_efficiency",0f)/100);long death=s.deathGeneration;
            var expected=new CharacterLocations.PlayerState(s.yaw,s.pitch,HealthRules.instant(s.hp,s.stats,contact.profile().heal()),s.handContainer,s.holstered,s.damageWall,s.stamina,s.sickness);
            Set<UUID> active=new HashSet<>();for(var area:grenadeAreas.states())active.add(area.projectile());
            locations.holdVitals(id);VitalsTask task=new VitalsTask(id,s,()->{});areaHealCommits.put(id,task);
            inventories.healArea(id,source,profile,active,expected,result->{
                if(!result.applied()){completeVitals(task,areaHealCommits);return;}
                areaHealApplied++;
                if(s.deathGeneration!=death||s.hp<=0){clearDeathEffects(s);completeVitals(task,areaHealCommits);return;}
                s.hp=Math.min(s.maxHp,Math.max(0,s.hp+healing));publishVitals(task,result.receipt(),playerState(s));
                if(!closed&&sessions.get(s.address)==s){vitals(s);syncAmmo(s);}
            },error->{areaHealDenied++;completeVitals(task,areaHealCommits);if(!closed&&sessions.get(s.address)==s)enqueue(s,0,GamePackets.chat(error));});
            return;
        }
    }
    private boolean reviveValid(ReviveCheck check,long now){Session target=check.target();return target.ready&&target.hp==0&&!target.respawning&&target.teleport==null&&target.gameplayMode==PlayerRules.Mode.STALKER&&target.life==check.life()&&Objects.equals(target.grenadeDeath,check.death())&&Arrays.equals(target.pos,check.position())&&sessions.get(target.address)==target&&now<check.source().started()+check.source().profiles().get(check.profile()).durationMillis();}
    private void advanceRevives(long wall){
        for(var entry:new ArrayList<>(reviveChecks.entrySet())){var check=entry.getValue();UUID id=entry.getKey();Session target=check.target();if(!reviveValid(check,wall)){reviveChecks.remove(id,check);check.placement().cancel(false);continue;}if(!check.placement().isDone())continue;
            reviveChecks.remove(id,check);int placed;try{placed=check.placement().join();}catch(CompletionException error){if(error.getCause() instanceof RejectedExecutionException){target.reviveCheckAt=wall+100;continue;}reviveErrors++;target.reviveCheckAt=check.source().until();continue;}
            if(placed!=1){target.reviveCheckAt=placed==0?wall+100:check.source().started()+check.source().profiles().get(check.profile()).durationMillis();if(placed!=0)reviveDenied++;continue;}
            if(inventories.busy(id)||vitalsPending(id)){target.reviveCheckAt=wall+25;continue;}
            refreshStats(target);var expected=new CharacterLocations.PlayerState(target.yaw,target.pitch,target.maxHp,target.handContainer,target.holstered,0,target.maxStamina,target.sickness);locations.holdVitals(id);var task=new VitalsTask(id,target,()->{});reviveCommits.put(id,task);
            inventories.reviveArea(id,check.source(),check.profile(),check.death(),grenadeAreas.states().stream().map(GrenadeAreaStore.Activation::projectile).collect(java.util.stream.Collectors.toSet()),wall,expected,result->{
                if(!result.applied()){completeVitals(task,reviveCommits);return;}
                // Pending commit fences automatic respawn and a new ready session for this character.
                if(target.life!=check.life()||!Objects.equals(target.grenadeDeath,check.death())){reviveErrors++;completeVitals(task,reviveCommits);return;}
                target.hp=expected.health();target.stamina=expected.stamina();target.damageWall=0;target.lastDamage=0;target.deadUntil=0;target.grenadeDeath=null;target.life++;target.velocity=new double[3];target.motion.reset();target.posture.reset();target.animation.reset();target.mode=0;target.speed=0;target.ground=true;target.reloadAt=0;target.swing=null;revives++;publishVitals(task,result.receipt(),expected);
                corpseDespawn(corpses.retire(target.lastCorpse));target.lastCorpse=null;
                if(!closed&&sessions.get(target.address)==target){enqueue(target,0,Wire.concat(packets.respawn(target.pos),packets.ready(target.pos,++tick,target.yaw,target.pitch,movementConfiguration(target)),GamePackets.velocityReset()));loadout(target);sendPermissions(target,true);enqueue(target,0,GamePackets.attributes(++tick,0,Factions.attributes(target.faction)));sendEffects(target,List.of(),target.effects.active(System.currentTimeMillis()));sendPulseEffects(target,List.of(),target.pulses.effects(System.currentTimeMillis()));sendSickness(target,true);resetPoses(target);vitals(target);}
            },error->{reviveDenied++;target.reviveCheckAt=check.source().until();completeVitals(task,reviveCommits);});
        }
        if(reviveChecks.size()>=32)return;
        for(Session target:List.copyOf(sessions.values())){UUID id=target.grant==null?null:character(target);if(id==null||!target.ready||target.hp!=0||target.grenadeDeath==null||target.respawning||target.teleport!=null||target.gameplayMode!=PlayerRules.Mode.STALKER||wall<target.reviveCheckAt||inventories.busy(id)||vitalsPending(id))continue;
            sourceLoop: for(var source:grenadeAreas.states())if(source.reviveTargets()!=null&&Objects.equals(source.reviveTargets().get(id),target.grenadeDeath))for(int profile=0;profile<source.profiles().size();profile++)if(GrenadeAreaComponents.revive(List.of(source.profiles().get(profile)))&&wall<source.started()+source.profiles().get(profile).durationMillis()&&!inventories.areaApplied(id,source.projectile(),profile)){
                if(reviveChecks.size()>=32)return;double[] at=target.pos.clone();reviveChecks.put(id,new ReviveCheck(target,source,profile,target.grenadeDeath,target.life,at,world.submit(owner->owner.nativePlacement(point(at)))));break sourceLoop;
            }
        }
    }
    private void applyPulseEffects(Session s,long wall){
        UUID id=character(s);if(s.hp<=0||s.respawning||s.teleport!=null||s.gameplayMode!=PlayerRules.Mode.STALKER||inventories.busy(id)||vitalsPending(id))return;
        for(var source:grenadeAreas.states())if(source.pulseTargets()!=null&&source.pulseTargets().contains(id)&&wall<source.until()&&GrenadeAreaScenes.pulse(source.profiles())&&!s.pulses.retired(source.projectile())&&!inventories.areaApplied(id,source.projectile(),0)){
            var scope=pulseScope(id);var preview=s.pulses.apply(source,0,scope.live(),wall);var beforePulses=s.pulses;s.pulses=preview;var stats=effectStats(s,s.effects,wall);s.pulses=beforePulses;long death=s.deathGeneration;var expected=new CharacterLocations.PlayerState(s.yaw,s.pitch,Math.min(s.hp,HealthRules.maximum(stats)),s.handContainer,s.holstered,s.damageWall,Math.min(s.stamina,staminaRules.capacity(stats)),s.sickness);locations.holdVitals(id);var task=new VitalsTask(id,s,()->{});pulseCommits.put(id,task);
            inventories.applyPulse(id,source,0,scope.live(),wall,expected,result->{
                if(!result.applied()){completeVitals(task,pulseCommits);return;}pulseApplied++;
                if(s.deathGeneration!=death||s.hp<=0){clearDeathEffects(s);completeVitals(task,pulseCommits);return;}
                var old=s.pulses.effects(Long.MIN_VALUE);s.pulses=result.after().expire(System.currentTimeMillis());refreshStats(s);publishVitals(task,result.receipt(),playerState(s));
                if(!closed&&sessions.get(s.address)==s){Set<Integer> previous=new HashSet<>(),current=new HashSet<>();old.forEach(e->previous.add(e.id()));var added=s.pulses.effects(System.currentTimeMillis());added.forEach(e->current.add(e.id()));sendPulseEffects(s,old.stream().filter(e->!current.contains(e.id())).toList(),added.stream().filter(e->!previous.contains(e.id())).toList());vitals(s);syncAmmo(s);}
            },error->{pulseDenied++;completeVitals(task,pulseCommits);if(!closed&&sessions.get(s.address)==s)enqueue(s,0,GamePackets.chat(error));});return;
        }
    }
    private void effectTick(Session s,long now){
        long wall=System.currentTimeMillis();var expired=s.effects.expire(wall);if(!expired.removed().isEmpty()){s.effects=expired.after();s.effectsExpireBefore=Math.max(s.effectsExpireBefore,wall);refreshStats(s);sendEffects(s,expired.removed(),List.of());vitals(s);}
        if(s.effectsExpireBefore!=0&&inventories.expireEffects(character(s),s.effectsExpireBefore))s.effectsExpireBefore=0;
        var pulseBefore=s.pulses.effects(Long.MIN_VALUE);var pulseAfter=s.pulses.expire(wall);if(pulseAfter.active(Long.MIN_VALUE).size()!=pulseBefore.size()){Set<Integer> held=new HashSet<>();pulseAfter.effects(Long.MIN_VALUE).forEach(e->held.add(e.id()));s.pulses=pulseAfter;s.effectsExpireBefore=Math.max(s.effectsExpireBefore,wall);refreshStats(s);sendPulseEffects(s,pulseBefore.stream().filter(e->!held.contains(e.id())).toList(),List.of());vitals(s);}
        if(s.hp<=0)return;applyPulseEffects(s,wall);advanceSickness(s,wall);if(s.hp<=0)return;float before=s.hp;
        s.hp=healthRules.step(s.hp,s.maxHp,s.stats,(now-s.lastDamage)/1e9,.05);
        if(s.hp==0){playerDeath(s,null,"Окружение",null,new double[3]);vitals(s);}
        else if(s.hp!=before){savePlayerState(s);if(now-s.lastVitals>=200_000_000){s.lastVitals=now;vitals(s);}}
    }
    private void staminaTick(Session s,long now){
        if(s.hp<=0)return;float before=s.stamina;if(s.gameplayMode==PlayerRules.Mode.ADMIN)s.stamina=s.maxStamina;else s.stamina=staminaRules.step(s.stamina,s.maxStamina,now-s.lastInput>=500_000_000?0:s.mode,s.stats,.05);
        if(s.stamina!=before)savePlayerState(s);
        if(s.reportedStamina!=null&&now-s.lastStaminaCorrection>=500_000_000){
            if(Math.abs(s.stamina-s.reportedStamina)>.08f)enqueue(s,0,Wire.dynamic("1455734411",Wire.bytes(o->o.writeFloat(staminaRules.correction(s.stamina,s.reportedStamina,s.maxStamina)))));
            s.reportedStamina=null;s.lastStaminaCorrection=now;
        }
    }
    private static Inventory.Address nullableAddress(DataInputStream in)throws IOException{int present=in.readUnsignedByte();if(present>1)throw new IOException("inventory_presence");return present==0?null:new Inventory.Address(in.readUnsignedByte(),in.readUnsignedByte(),in.readUnsignedShort());}
    private Inventory inventory(Session s){return inventories.inventory(character(s));}
    private void warmAppearances()throws Exception {
        // 1160390453 writes only ID and the three common flags when the real
        // factory does not implement 989274007. Resolve that contract offline.
        for(int id=1;id<65536;id++){
            if(id==29998||id==29999)continue; // Internal climb/parkour actions, not inventory factories.
            try{inventories.catalog().get(id);}catch(IllegalArgumentException absent){continue;}
            if(!appearances.containsKey(id)&&!inventories.hasAppearanceCodec(id)){
                int factory=id;appearances.put(id,Wire.bytes(out->{out.writeInt(factory);out.write(new byte[3]);}));
            }
        }
    }
    private Map<Integer,byte[]> equipment(Session s){Map<Integer,byte[]> attributes=new TreeMap<>();for(int container=0;container<12;container++){ItemStack stack=inventory(s).at(new Inventory.Address(0,container,0));if(stack==null)attributes.put(55+container,null);else{byte[] appearance=inventories.appearance(character(s),container);if(appearance==null)throw new IllegalArgumentException("equipment_appearance_missing:"+stack.id());appearance=appearance.clone();int mask=stack.equipmentMask();appearance[4]=(byte)((mask&(1<<8))!=0?1:0);appearance[5]=(byte)((mask&(1<<9))!=0?1:0);appearance[6]=(byte)((mask&(1<<11))!=0?1:0);attributes.put(55+container,appearance);}}return attributes;}
    private boolean openInventory(Session s){
        if(!InventoryScreen.available(s.hp,s.respawning))return false;
        enqueue(s,0,InventoryScreen.player(inventory(s),GamePackets.attributes(tick,0,equipment(s))));return true;
    }
    private boolean openStorage(Session s,int page,boolean refreshPlayer){
        var plan=s.inventoryScreen.prepare(page,refreshPlayer);if(!InventoryScreen.available(s.hp,s.respawning))return false;
        var root=inventory(s);byte[] attributes=plan.refreshPlayer()?GamePackets.attributes(tick,0,equipment(s)):new byte[0];
        enqueue(s,0,InventoryScreen.storage(root,plan,attributes));s.inventoryScreen.opened(plan);return true;
    }
    private void inventorySnapshot(Session s,int owner){if(owner==0)enqueue(s,0,InventoryPackets.snapshot(inventory(s),0,0,0));else enqueue(s,0,InventoryPackets.snapshot(inventory(s),-1,0,s.inventoryScreen.page()));enqueue(s,0,InventoryPackets.snapshot(inventory(s),owner,1,0));}
    private void refreshWeapon(Session s,Runnable ready){if(sessions.get(s.address)!=s)return;cancelUnjam(s);long inventoryRevision=inventory(s).revision();cancelLauncherReload(s);inventories.weapon(character(s),s.handContainer,weapon->{if(sessions.get(s.address)!=s)return;if(s.ready&&inventory(s).revision()!=inventoryRevision)inventorySnapshot(s,0);refreshStats(s);s.weapon=weapon;s.rounds=weapon==null?0:weapon.rounds();s.firearm=weapon==null?inventories.identity(character(s),s.handContainer):weapon.identity();s.magazines=weapon==null?null:weapon.magazines();s.reloadAt=0;s.animation.cancel();s.physicalCycle=weapon==null?null:PhysicalReload.activeSaved(inventories.gameplay(character(s)),s.firearm);refreshReserve(s);ready.run();},error->{enqueue(s,0,GamePackets.chat(error));remove(s);});}
    private void refreshReserve(Session s){s.reserve=s.weapon==null?0:inventory(s).items().entrySet().stream().filter(e->e.getKey().aggregate()==1&&e.getValue().id()==s.weapon.ammo()).mapToInt(e->e.getValue().count()).sum();}
    private void syncHand(Session s){byte[] owner=GamePackets.attributes(tick,0,hand(s));enqueue(s,0,owner);if(s.ready){byte[] remote=GamePackets.attributes(tick,s.entity,hand(s));for(Session other:sessions.values())if(other!=s&&other.ready)enqueue(other,0,remote);}}
    private void inventoryChanged(Session s,Inventory.Plan plan,Runnable done){
        if(closed||sessions.get(s.address)!=s){done.run();return;}
        for(var e:plan.changes().entrySet()){Inventory.Address a=e.getKey();if(a.aggregate()<2)enqueue(s,0,InventoryPackets.point(0,-1,a,e.getValue()));if(s.inventoryScreen.storageOpen()&&(a.aggregate()==1||a.aggregate()==2&&a.container()==s.inventoryScreen.page()))enqueue(s,0,InventoryPackets.point(-1,-1,a,e.getValue()));}
        if(plan.changes().keySet().stream().anyMatch(a->a.aggregate()==0)){refreshWeapon(s,()->{enqueue(s,0,GamePackets.attributes(tick,0,equipment(s)));inventorySnapshot(s,0);syncHand(s);syncAmmo(s);vitals(s);for(Session other:sessions.values())if(other!=s&&other.ready)enqueue(other,0,Wire.concat(GamePackets.attributes(tick,s.entity,equipment(s)),GamePackets.attributes(tick,s.entity,Factions.attributes(s.faction))));done.run();});}
        else {refreshReserve(s);done.run();}
    }
    private void inventorySort(Session s,byte[] body)throws Exception{
        Inventory.Address target=InventoryPackets.sort(body,s.inventoryScreen.page(),s.inventoryScreen.storageOpen());boolean storage=target.aggregate()==2;
        if(storage&&(!s.inventoryScreen.storageOpen()||target.container()!=s.inventoryScreen.page())){inventorySnapshot(s,-1);return;}
        if(!InventoryScreen.available(s.hp,s.respawning)){inventorySnapshot(s,storage?-1:0);return;}
        inventories.mutate(character(s),before->before.sort(target),plan->{
            if(plan.changes().size()>32)inventorySnapshot(s,storage?-1:0);else inventoryChanged(s,plan,()->{});
        },error->inventorySnapshot(s,storage?-1:0));
    }
    private void inventoryRequest(Session s,int id,byte[] body)throws Exception{
        if(BoltCosmetics.handle(this,s,id,body))return;
        InventoryPackets.Request request=InventoryPackets.read(id,body,s.inventoryScreen.page());int owner=request.owner(),sequence=request.sequence();
        java.util.function.Consumer<String> reject=error->{inventorySnapshot(s,owner);enqueue(s,0,InventoryPackets.ack(owner,sequence));};
        if(owner==-1&&(!s.inventoryScreen.storageOpen()||request.source().aggregate()==0||request.destinations().stream().anyMatch(a->a.aggregate()==0))||owner==0&&(request.source().aggregate()==2||request.destinations().stream().anyMatch(a->a.aggregate()==2))){reject.accept("inventory_owner");return;}
        inventories.mutate(character(s),before->{if(request.destinations().isEmpty())throw new IllegalArgumentException("drop_not_available");if(id==2009){if(request.targetAggregate()<0||request.targetAggregate()>1||(request.source().aggregate()==2?0:request.source().aggregate())==request.targetAggregate())throw new IllegalArgumentException("quick_aggregate");for(Inventory.Address a:request.destinations())if((a.aggregate()==2?0:a.aggregate())!=request.targetAggregate())throw new IllegalArgumentException("quick_aggregate");return before.quick(request.source(),request.destinations(),request.count());}return before.move(request.source(),request.destinations().get(0),request.count(),request.split());},plan->inventoryChanged(s,plan,()->enqueue(s,0,InventoryPackets.ack(owner,sequence))),reject);
    }
    private double[] configuredSpawn(Session s){return regions==null?new double[]{spawn[0]+(s.entity-2)*5,spawn[1],spawn[2]}:spawn.clone();}
    private double[] spawn(Session s){return vector(locations.spawn(character(s),point(configuredSpawn(s))));}
    private static World.Vec point(double[] p){return new World.Vec(p[0],p[1],p[2]);}
    private static double[] vector(World.Vec p){return new double[]{p.x(),p.y(),p.z()};}
    private double[][] placementCandidates(Session s,boolean restore){LinkedHashSet<World.Vec> values=new LinkedHashSet<>();World.Vec saved=restore?locations.position(character(s)):null;if(saved!=null)values.add(saved);values.addAll(locations.spawnCandidates(character(s),point(configuredSpawn(s))));return values.stream().map(GameServer::vector).toArray(double[][]::new);}
    private void beginPlacement(Session s,double[][] candidates){s.candidates=candidates;s.candidate=0;s.positioned=false;s.placement=null;s.placementDeadline=System.nanoTime()+3_000_000_000L;s.nextPlacement=0;}
    private void restoreJoinState(Session s){s.configuredScale=inventories.scale(character(s)).factor();s.characterScale=1;s.grenadeDeath=CharacterRevival.read(inventories.gameplay(character(s))).filter(CharacterRevival::active).map(CharacterRevival::death).orElse(null);s.pulses=inventories.pulseEffects(character(s),System.currentTimeMillis());s.effects=CharacterEffects.read(inventories.gameplay(character(s)),medicines,System.currentTimeMillis());refreshStats(s);s.restoredState=locations.state(character(s));if(s.restoredState!=null){s.sickness=s.restoredState.sickness().restored(sicknessRules,System.currentTimeMillis());s.selectedAreas=s.gameplayMode==PlayerRules.Mode.STALKER&&s.restoredState.health()>0?AreaExposure.select(grenadeAreaScenes.contacts(grenadeAreas.states(),s.pos,CharacterScale.height(s.posture.locomotion(),s.characterScale),System.currentTimeMillis()),areaRelations,areaActor(s),this::liveAreaOwner,AreaRelations.Mode.DEFAULT):List.of();s.sicknessExposure=AreaExposure.bank(s.selectedAreas);refreshStats(s);s.hp=Math.min(s.maxHp,s.restoredState.health());s.stamina=Math.min(s.maxStamina,s.restoredState.stamina());s.observedStamina=s.stamina;s.restoredState=null;}}
    private void completeJoin(Session s){if(sessions.get(s.address)==s&&!s.ready&&s.inventoryReady&&s.positioned&&!vitalsPending(character(s))&&!locations.stateSavePending(character(s))){restoreJoinState(s);join(s,s.loginProtocol);if(s.hp==0){if(s.grenadeDeath==null)s.grenadeDeath=UUID.randomUUID();s.deadUntil=System.nanoTime()+CorpseLifecycle.RESPAWN_NANOS;clearDeathEffects(s);enqueue(s,0,Wire.concat(GamePackets.death("Сохранённое состояние",s.pos),GamePackets.respawnOptions()));}}}
    private void advanceRespawnState(Session s){
        if(s.respawnStateSave==null||!s.respawnStateSave.isDone())return;
        try{s.respawnStateSave.join();}catch(CompletionException failure){s.respawnStateSave=null;remove(s);return;}
        s.respawnStateSave=null;
        s.respawning=false;s.deadUntil=0;s.velocity=new double[3];s.motion.reset();respawns++;s.chunks.clear();s.chunkRevisions.clear();s.cachedRequests.clear();s.deliveries.clear();s.faults.clear();s.manifestRetryAt=0;s.deltas=null;s.completeTiles=null;s.completeTileAt=0;s.pendingChunks.clear();s.chunkX=Integer.MIN_VALUE;s.window.reset();s.windowEpoch++;enqueue(s,0,packets.respawn(s.pos));enqueue(s,0,scaledReady(s));loadout(s);sendPermissions(s,true);enqueue(s,0,GamePackets.attributes(++tick,0,Factions.attributes(s.faction)));for(Session other:sessions.values())if(other!=s&&other.ready)enqueue(other,0,GamePackets.vitals(tick,s.entity,s.hp,s.maxHp,s.animation.aiming()));
    }
    private void advancePlacement(Session s,long now){
        if(s.candidates==null)return;
        if(s.placement!=null){if(!s.placement.isDone())return;int result;try{result=s.placement.join();}catch(Exception failure){result=failure.getCause() instanceof RejectedExecutionException?0:2;}s.placement=null;
            if(result==1){s.pos=s.candidates[s.candidate];s.candidates=null;s.positioned=true;
                if(s.respawning){s.pulses=inventories.pulseEffects(character(s),System.currentTimeMillis());s.effects=CharacterEffects.read(inventories.gameplay(character(s)),medicines,System.currentTimeMillis());refreshStats(s);s.hp=s.maxHp;s.damageWall=0;s.lastDamage=0;s.stamina=s.maxStamina;locations.observe(character(s),point(s.pos));s.respawnStateSave=savePlayerState(s);}else completeJoin(s);return;}
            if(result==2||now>=s.placementDeadline){s.candidate++;s.placementDeadline=now+3_000_000_000L;if(s.candidate>=s.candidates.length){if(s.respawning){s.respawning=false;s.candidates=null;s.positioned=true;s.deadUntil=now+2_000_000_000L;respawnRetries++;}else remove(s);return;}}
            s.nextPlacement=now+100_000_000L;
        }
        if(now>=s.nextPlacement){World.Vec candidate=point(s.candidates[s.candidate]);s.placement=world.submit(w->w.nativePlacement(candidate));}
    }
    private static long chunkKey(int x,int z){return ((long)x<<32)|(z&0xffffffffL);}
    private void chunk(Session s,int x,int z){
        chunkWindow(s);long key=chunkKey(x,z);if(!s.window.inside(key))return;if(!s.chunks.contains(key)&&s.pendingChunks.size()<256)s.pendingChunks.add(key);
    }
    private void demandChunk(Session s,ChunkManifestPackets.Request request){
        chunkWindow(s);long key=chunkKey(request.x(),request.z());if(!s.window.inside(key))return;
        if(s.deliveries.current(key,terrainProjection.version(request.x(),request.z()),System.nanoTime())){chunkDuplicateDemands++;s.window.complete(key);return;}
        if(s.pendingChunks.contains(key)){if(request.revision()==null||s.cachedRequests.get(key)!=null)s.cachedRequests.put(key,request.revision());return;}
        if(s.pendingChunks.size()>=256){chunkDeferredDemands++;s.chunks.remove(key);s.chunkRevisions.remove(key);s.deliveries.remove(key);if(s.deltas!=null&&s.deltaChunk==key)s.deltas=null;s.window.retry(List.of(key));return;}
        s.pendingChunks.add(key);s.cachedRequests.put(key,request.revision());
    }
    private void releaseChunk(Session s,long key){
        s.pendingChunks.remove(key);s.cachedRequests.remove(key);s.chunks.remove(key);s.chunkRevisions.remove(key);s.deliveries.remove(key);if(s.deltas!=null&&s.deltaChunk==key)s.deltas=null;s.window.retry(List.of(key));chunkReleased++;
    }
    private void chunkWindow(Session s){
        var drone=drones.owned(s);var focus=VehicleCoastView.update(s.window,s.pos,drone==null?null:drone.state(),drone!=null&&drone.piloting(),vehicles.all().stream().filter(c->c.driver==null&&c.simulator==s&&c.state.speed>.05).map(c->c.state).toList());
        int cx=focus.x(),cz=focus.z();if(cx==s.chunkX&&cz==s.chunkZ){if(focus.changed()){s.windowEpoch++;pruneChunkWindow(s);}return;}
        s.chunkX=cx;s.chunkZ=cz;
        s.windowEpoch++;
        pruneChunkWindow(s);
        for(int radius=0;radius<=2;radius++)for(int dz=-radius;dz<=radius;dz++)for(int dx=-radius;dx<=radius;dx++)if(Math.max(Math.abs(dx),Math.abs(dz))==radius)chunk(s,cx+dx,cz+dz);
    }
    private void pruneChunkWindow(Session s){
        s.faults.prune(s.window);
        s.pendingChunks.removeIf(key->!s.window.inside(key));s.cachedRequests.keySet().retainAll(s.pendingChunks);
        for(int n=0,limit=Math.min(ChunkWindow.WORK_BUDGET,s.chunks.size());n<limit;n++){Long key=s.chunks.scan();if(!s.window.inside(key)){s.chunks.remove(key);s.chunkRevisions.remove(key);s.deliveries.remove(key);}}
        if(s.deltas!=null&&!s.window.inside(s.deltaChunk))s.deltas=null;
    }
    private void streamManifest(Session s){
        if(s.manifest!=null){
            if(!s.manifest.isDone())return;List<ChunkManifests.Row> rows;
            try{rows=s.manifest.join();}catch(CompletionException failure){manifestFailures++;s.window.retry(s.manifestKeys);s.manifestRetryAt=System.nanoTime()+1_000_000_000L;s.manifest=null;s.manifestKeys=List.of();return;}
            if(s.manifestEpoch!=s.windowEpoch){manifestStale++;s.window.retry(s.manifestKeys);s.manifest=null;s.manifestKeys=List.of();return;}
            List<ChunkManifestPackets.Entry> entries=new ArrayList<>();
            for(var row:rows){if(row.error()!=null){manifestFailures++;s.faults.failed(row.key(),System.nanoTime());s.window.complete(row.key());continue;}s.faults.resolved(row.key());if(!row.present()||s.chunks.contains(row.key())){s.window.complete(row.key());continue;}
                // A dirty overlay has another native revision. A null catalogue revision requests
                // the authoritative projected body instead of comparing an obsolete base UUID.
                UUID revision=terrainProjection.version((int)(row.key()>>32),(int)row.key())==0?row.revision():null;
                entries.add(new ChunkManifestPackets.Entry((int)(row.key()>>32),(int)row.key(),revision));
            }
            if(!entries.isEmpty()){byte[] frame=ChunkManifestPackets.frame(entries);if(s.lanes[1].queued.size()+(frame.length+FRAGMENT-1)/FRAGMENT>256)return;enqueue(s,1,frame);manifestsSent++;manifestEntries+=entries.size();}
            s.manifest=null;s.manifestKeys=List.of();
        }
        long now=System.nanoTime();if(now<s.manifestRetryAt)return;s.window.faultRetry(s.faults.ready(now,Math.min(s.window.faultCapacity(),Math.min(32,Math.max(1,chunkSettings.batchSize()/4)))));List<Long> keys=s.window.claim(chunkSettings.batchSize());if(!keys.isEmpty()){s.manifestKeys=keys;s.manifestEpoch=s.windowEpoch;s.manifest=chunkManifests.inspect(keys);}
    }
    private void streamChunks(Session s){
        if(s.teleport!=null)return; // Finish existing frames; never truncate an in-flight stream.
        if(!streamCompleteTiles(s))return;
        if(!streamNativeRepair(s))return;chunkWindow(s);pruneChunkWindow(s);Lane lane=s.lanes[1];if(lane.queued.size()>32||lane.sent.size()>32)return;
        // Changes in already visible chunks precede background map loading on the same lane.
        streamBlockChanges(s);if(s.completeTiles!=null||lane.queued.size()>32||lane.sent.size()>32)return;
        streamManifest(s);int sent=0,requested=0;
        List<Long> deferred=new ArrayList<>();
        for(var iterator=s.pendingChunks.iterator();iterator.hasNext()&&sent<2&&requested<16;){long key=iterator.next();int x=(int)(key>>32),z=(int)key;requested++;
            TerrainProjection.Prepared prepared=terrainProjection.prepared(x,z);if(prepared==null){iterator.remove();deferred.add(key);continue;}
            // Every initial/re-entry demand needs the current immutable projection. Older
            // snapshots cannot be repaired by deltas that a native commit already retired.
            if(prepared.revision()!=terrainProjection.version(x,z)){iterator.remove();deferred.add(key);continue;}
            UUID cached=s.cachedRequests.get(key);byte[] frame=prepared.frame(cached);
            if(lane.queued.size()+(frame.length+FRAGMENT-1)/FRAGMENT>256)break;
            if(!s.deliveries.claim(key,prepared.nativeRevision(),prepared.revision(),System.nanoTime())){chunkDuplicateDemands++;s.window.complete(key);s.cachedRequests.remove(key);iterator.remove();continue;}
            if(prepared.nativeRevision().equals(cached))chunkCacheHits++;else chunkFullResponses++;
            enqueue(s,1,frame);s.cachedRequests.remove(key);s.chunks.add(key);s.window.delivered(key);s.chunkRevisions.put(key,prepared.revision());iterator.remove();sent++;
            if(prepared.completeFrameCount()>0){s.completeTiles=prepared;s.completeTileAt=0;streamCompleteTiles(s);break;}
        }
        s.pendingChunks.addAll(deferred);if(s.completeTiles==null)streamBlockChanges(s);
    }
    /** Complete NBT frames follow their atomic appearance chunk on the same reliable lane. */
    private boolean streamCompleteTiles(Session s){
        if(s.completeTiles==null)return true;Lane lane=s.lanes[1];int budget=2;
        while(s.completeTileAt<s.completeTiles.completeFrameCount()&&budget-->0){byte[] frame=s.completeTiles.completeFrame(s.completeTileAt);int fragments=(frame.length+FRAGMENT-1)/FRAGMENT;if(lane.queued.size()>32||lane.sent.size()>32||lane.queued.size()+fragments>MAX_QUEUE)return false;enqueue(s,1,frame);s.completeTileAt++;}
        if(s.completeTileAt==s.completeTiles.completeFrameCount()){s.completeTiles=null;s.completeTileAt=0;return true;}return false;
    }
    private void streamBlockChanges(Session s){
        if(s.deltas==null){for(int n=0,limit=Math.min(ChunkWindow.WORK_BUDGET,s.chunks.size());n<limit;n++){long key=s.chunks.scan(1);if(!s.window.inside(key))continue;int x=(int)(key>>32),z=(int)key;long sent=s.chunkRevisions.getOrDefault(key,0L);if(terrainProjection.version(x,z)<=sent)continue;
            TerrainProjection.Changes changes=terrainProjection.changes(x,z,sent);
            if(changes==null)continue;
            if(changes.usable(sent)){
                byte[] frame=BlockDeltaPackets.frame(x,z,changes,sent);Lane output=s.lanes[1];int fragments=(frame.length+FRAGMENT-1)/FRAGMENT;
                if(output.queued.size()>32||output.sent.size()>32||output.queued.size()+fragments>MAX_QUEUE)return;
                if(frame.length>0)enqueue(s,1,frame);s.chunkRevisions.put(key,changes.revision());return;
            }
            TerrainProjection.Prepared prepared=terrainProjection.prepared(x,z);if(prepared==null||prepared.revision()<=sent)continue;s.deltaChunk=key;s.deltas=prepared;s.deltaAt=prepared.after(sent);break;}}
        if(s.deltas==null)return;Lane lane=s.lanes[1];
        if(s.deltas.refreshChunk(s.deltaAt,s.chunkRevisions.getOrDefault(s.deltaChunk,0L))){
            // Called once per gameplay tick. Queue/window backpressure bounds transport work;
            // a global 250ms delay made a hundred dirty chunks take over25 seconds to arrive.
            long now=System.nanoTime();
            TerrainProjection.Prepared latest=terrainProjection.prepared((int)(s.deltaChunk>>32),(int)s.deltaChunk);if(latest!=null&&latest.revision()>s.deltas.revision())s.deltas=latest;
            byte[] frame=s.deltas.frame();if(lane.queued.size()>32||lane.sent.size()>32||lane.queued.size()+(frame.length+FRAGMENT-1)/FRAGMENT>256)return;
            enqueue(s,1,frame);s.completeTiles=s.deltas.completeFrameCount()>0?s.deltas:null;s.completeTileAt=0;streamCompleteTiles(s);if(s.deltas.tileRefresh())tileRefreshes++;else bulkBlockRefreshes++;s.deliveries.record(s.deltaChunk,s.deltas.nativeRevision(),s.deltas.revision(),now);s.chunkRevisions.put(s.deltaChunk,s.deltas.revision());s.deltas=null;return;
        }
        int budget=32;
        while(s.deltaAt<s.deltas.deltas().size()&&budget-->0&&lane.queued.size()<256){var delta=s.deltas.deltas().get(s.deltaAt++);int x=((int)(s.deltaChunk>>32))*16+(delta.cell()&15),z=((int)s.deltaChunk)*16+((delta.cell()>>>4)&15);enqueue(s,1,GamePackets.block(x,delta.cell()>>>8,z,delta.voxel().id(),delta.voxel().metadata()));}
        if(s.deltaAt==s.deltas.deltas().size()){s.chunkRevisions.put(s.deltaChunk,s.deltas.revision());s.deltas=null;}
    }
    private void advanceSpawn(Session s,long now){
        if(s.requestedSpawn==null)return;
        if(s.spawnCheck!=null){if(!s.spawnCheck.isDone())return;int result;try{result=s.spawnCheck.join();}catch(Exception failed){result=failed.getCause() instanceof RejectedExecutionException?0:2;}s.spawnCheck=null;
            if(result==1){s.spawnSave=locations.setSpawn(s.spawnScope,s.requestedSpawn);s.requestedSpawn=null;return;}
            if(result==2||now>=s.spawnDeadline){s.requestedSpawn=null;enqueue(s,0,GamePackets.chat("Точка спавна недоступна или перекрыта блоками."));return;}s.nextSpawnCheck=now+100_000_000L;
        }
        if(now>=s.nextSpawnCheck){World.Vec target=s.requestedSpawn;s.spawnCheck=world.submit(w->w.nativePlacement(target));}
    }
    private void teleport(Session s,TeleportCommands.Request request,String reply){
        if(vehicles.driving(s)!=null)throw new IllegalArgumentException("exit_vehicle_first");if(s.teleport!=null)throw new IllegalArgumentException("teleport_busy");if(s.respawning)throw new IllegalArgumentException("teleport_not_available");
        World.Vec target=request.position();if(request.player()!=null){String name=TeleportCommands.foldName(request.player());Session other=sessions.values().stream().filter(p->p.ready&&!p.respawning&&TeleportCommands.foldName(p.grant.name()).equals(name)).findFirst().orElseThrow(()->new IllegalArgumentException("teleport_player_not_found"));target=point(other.pos);}
        CharacterLocations.validPosition(target);s.portalLookup.clear();s.teleport=new Teleport(target,request.surface(),reply,System.nanoTime());
    }
    private CompletableFuture<World.PortalQuery> queryPortal(PortalLookup.Request request){
        portalQueries++;return world.submit(owner->owner.portalQuery(request.source(),request.interaction(),null,request.selector()));
    }
    private void requestPortal(Session s,boolean interaction){
        long now=System.nanoTime();if(!s.ready||s.respawning||s.teleport!=null||now<s.portalUntil)return;
        s.portalLookup.request(point(s.pos),interaction,s.life,ThreadLocalRandom.current().nextInt(1_000_000)/1_000_000d,now,this::queryPortal);
    }
    private void advancePortal(Session s,long now){
        if(!s.portalLookup.busy())return;try{var result=s.portalLookup.advance(point(s.pos),s.life,s.ready&&!s.respawning&&s.teleport==null&&now>=s.portalUntil,now,world::portalEpoch,this::queryPortal);
        if(result.failed()){portalErrors++;enqueue(s,0,GamePackets.chat("portal_activation_failed"));return;}
        var activation=result.activation();if(activation!=null){s.portalUntil=now+(long)(activation.portal().parameters().cooldown()*1_000_000_000L);teleport(s,new TeleportCommands.Request(activation.target(),null,false),"Переход через портал выполнен.");portalActivations++;}}catch(RuntimeException invalid){s.portalLookup.clear();portalErrors++;enqueue(s,0,GamePackets.chat("portal_activation_failed"));}
    }
    private void advanceTeleport(Session s,long now){
        Teleport op=s.teleport;if(op==null)return;
        try{
            if(op.phase<3&&now>op.deadline)throw new IllegalArgumentException("teleport_terrain_or_transport_timeout");
            if(op.phase==0){
                if(op.surface){if(op.height!=null){if(!op.height.isDone())return;int y=op.height.join();op.height=null;if(y>=0){op.target=new World.Vec(op.target.x(),y,op.target.z());op.surface=false;}else op.nextRetry=now+100_000_000L;}if(op.surface){if(now>=op.nextRetry){int x=(int)Math.floor(op.target.x()),z=(int)Math.floor(op.target.z());op.height=world.submit(owner->owner.surfaceHeight(x,z));}return;}}
                op.center=terrainProjection.prepared((int)Math.floor(op.target.x()/16),(int)Math.floor(op.target.z()/16));if(op.center==null)return;op.phase=1;
            }
            Lane lane=s.lanes[1];if(op.phase==1){if(!streamCompleteTiles(s))return;if(!lane.queued.isEmpty()||!lane.sent.isEmpty()||inventories.busy(character(s)))return;enqueue(s,1,op.center.frame());s.completeTiles=op.center.completeFrameCount()>0?op.center:null;s.completeTileAt=0;op.phase=2;return;}
            if(op.phase==2){if(!streamCompleteTiles(s))return;if(!lane.queued.isEmpty()||!lane.sent.isEmpty()||inventories.busy(character(s)))return;op.save=locations.teleport(character(s),op.target);op.phase=3;return;}
            if(!op.save.isDone()||inventories.busy(character(s)))return;op.save.join();
            s.pos=vector(op.target);s.life++;s.speed=0;s.posture.reset();s.mode=0;s.ground=true;s.animation.reset();s.teleportGuard.install(op.target,now);
            s.chunks.clear();s.chunkRevisions.clear();s.cachedRequests.clear();s.deliveries.clear();s.faults.clear();s.manifestRetryAt=0;s.deltas=null;s.completeTiles=null;s.completeTileAt=0;s.pendingChunks.clear();s.chunkX=Integer.MIN_VALUE;s.window.reset();s.windowEpoch++;
            long key=chunkKey((int)Math.floor(op.target.x()/16),(int)Math.floor(op.target.z()/16));s.chunks.add(key);s.chunkRevisions.put(key,op.center.revision());s.bots.clear();s.botHealth.clear();s.botMaximum.clear();s.botAppearances.clear();s.botReplication.reset();
            s.velocity=new double[3];s.motion.reset();enqueue(s,0,Wire.concat(scaledReady(s),GamePackets.velocityReset()));loadout(s);sendPermissions(s,true);enqueue(s,0,GamePackets.attributes(++tick,0,Factions.attributes(s.faction)));sendEffects(s,List.of(),s.effects.active(System.currentTimeMillis()));sendPulseEffects(s,List.of(),s.pulses.effects(System.currentTimeMillis()));sendSickness(s,true);
            for(Session other:sessions.values())if(other!=s&&other.ready){enqueue(other,0,GamePackets.teleportMovement(tick,s.entity,s.pos,s.yaw,s.pitch));enqueue(s,0,Wire.concat(Wire.frame(2028,Wire.bytes(out->out.writeInt(other.entity))),GamePackets.spawn(other.entity,other.grant.name(),other.pos,other.yaw,other.pitch),GamePackets.attributes(tick,other.entity,equipment(other)),GamePackets.attributes(tick,other.entity,hand(other)),GamePackets.attributes(tick,other.entity,Factions.attributes(other.faction)),GamePackets.vitals(tick,other.entity,visibleHealth(other),other.maxHp,other.animation.aiming())));}
            resetPoses(s);s.teleport=null;teleports++;chunkWindow(s);enqueue(s,0,GamePackets.chat(op.reply));if(op.respawnRequested&&s.hp<=0)beginRespawn(s);
        }catch(Exception error){s.teleport=null;teleportErrors++;enqueue(s,0,GamePackets.chat(Objects.toString(error.getMessage(),"teleport_failed")));if(op.respawnRequested&&s.hp<=0)beginRespawn(s);}
    }
    private void beginRespawn(Session s){s.grenadeDeath=null;s.motion.reset();s.animation.reset();s.posture.reset();s.mode=0;s.speed=0;s.ground=true;s.portalLookup.clear();s.teleportGuard.clear();s.respawning=true;s.life++;beginPlacement(s,placementCandidates(s,false));}
    private boolean piloting(Session s){var drone=drones.owned(s);return vehicles.driving(s)!=null||drone!=null&&drone.piloting();}
    private byte[] animation(Session s){boolean window=vehicleActions.windowView(s,System.nanoTime());return PlayerAnimationPackets.component(s.animation.snapshot(window?0:s.posture.locomotion(),window?0:s.posture.previous(),window?0:s.mode,window?0:CharacterScale.animationSpeed(s.speed,s.characterScale),s.ground,s.yaw,piloting(s)&&!window));}
    private void fpvPose(Session s){s.motion.reset();s.velocity=new double[3];s.animation.reset();s.posture.set(piloting(s)?11:0,piloting(s)?11:0);s.mode=0;s.speed=0;for(Session viewer:new ArrayList<>(sessions.values()))if(viewer.ready)enqueueOrRetire(viewer,GamePackets.attributes(tick,viewer==s?0:s.entity,Map.of(201,new byte[]{0})));}
    private String effectivePose(Session s){var drone=drones.owned(s);return vehicles.driving(s)!=null?(vehicleActions.windowView(s,System.nanoTime())?null:"SIT"):drone!=null&&drone.piloting()?"pda_sit_idle":s.forcedAnimation;}
    private void pose(Session viewer,int entity,String name,boolean force){
        if(!force&&viewer.poses.containsKey(entity)&&Objects.equals(viewer.poses.get(entity),name))return;
        if(!force&&name==null&&!viewer.poses.containsKey(entity))return;
        enqueue(viewer,0,CommandSupport.pose(entity,++poseRevision,name));viewer.poses.put(entity,name);
    }
    private void playerPoses(Session viewer){pose(viewer,0,effectivePose(viewer),false);for(Session other:sessions.values())if(other!=viewer&&other.ready)pose(viewer,other.entity,effectivePose(other),false);}
    private void resetPoses(Session viewer){viewer.replication.reset();viewer.botReplication.reset();for(Session other:sessions.values())other.replication.forget(viewer.entity);viewer.poses.clear();clones.forget(viewer);pose(viewer,0,effectivePose(viewer),true);}
    private byte[] scaledReady(Session s){return packets.ready(s.pos,tick,s.yaw,s.pitch,movementConfiguration(s),s.characterScale);}
    private void sendScale(Session viewer,Session actor){if(viewer.scaleCapable)enqueueOrRetire(viewer,CharacterScale.frame(viewer==actor?0:actor.entity,++scaleRevision,new playerscale.ScaleValue(actor.characterScale)));}
    private void sendCloneScale(Session viewer,PlayerClones.Clone<Session> clone){if(viewer.scaleCapable)enqueueOrRetire(viewer,CharacterScale.frame(clone.entity,++scaleRevision,new playerscale.ScaleValue(clone.state.scale)));}
    private void removeScale(Session viewer,int entity){if(viewer.scaleCapable)enqueueOrRetire(viewer,CharacterScale.remove(entity,++scaleRevision));}
    private void scaleHello(Session s,byte[] body){
        playerscale.ScaleProtocol.hello(body);long now=System.nanoTime();if(!s.ready||now-s.scaleHelloAt<250_000_000L)return;s.scaleHelloAt=now;boolean first=!s.scaleCapable;s.scaleCapable=true;
        for(Session actor:List.copyOf(sessions.values()))if(actor.ready)sendScale(s,actor);
        for(var clone:clones.all())if(clone.recipients.contains(s))sendCloneScale(s,clone);
        if(first&&s.configuredScale!=s.characterScale)s.scaleRestore=true;
    }
    private void requestScale(Session requester,CharacterScale.Command request){
        if(!playerRules.entitled(character(requester)))throw new IllegalArgumentException("scale_requires_admin");
        Session target=requester;if(request.target()!=null){target=null;for(Session candidate:sessions.values())if(candidate.ready&&candidate.grant.name().equalsIgnoreCase(request.target())){if(target!=null)throw new IllegalArgumentException("scale_target_ambiguous");target=candidate;}}
        if(target==null||!target.ready||target.hp<=0||target.respawning||target.teleport!=null||target.scaleSaving||target.scaleChange!=null||inventories.busy(character(target)))throw new IllegalArgumentException("scale_target_not_ready");
        if(!target.scaleCapable&&request.scale().factor()!=1)throw new IllegalArgumentException("scale_client_required");
        if(vehicles.driving(target)!=null)throw new IllegalArgumentException("exit_vehicle_first");
        // A grounded native controller retains downward gravity velocity after clipping.
        if(!target.ground||target.speed>.05)throw new IllegalArgumentException("scale_stand_still");
        double[] at=target.pos.clone();double radius=CharacterScale.radius(request.scale().factor()),height=CharacterScale.height(0,request.scale().factor());
        target.scaleRestore=false;target.scaleChange=new ScaleChange(requester,at,request.scale(),world.submit(w->w.characterResize(point(at),radius,height)).thenCompose(future->future),System.nanoTime()+3_000_000_000L);
    }
    private void advanceScale(Session target,long now){
        if(target.scaleRestore&&target.scaleChange==null&&!target.scaleSaving&&target.ready&&target.ground&&target.speed<=.05&&target.hp>0&&!target.respawning&&target.teleport==null&&vehicles.driving(target)==null){
            target.scaleRestore=false;double[] at=target.pos.clone();var scale=new playerscale.ScaleValue(target.configuredScale);
            target.scaleChange=new ScaleChange(null,at,scale,world.submit(w->w.characterResize(point(at),CharacterScale.radius(scale.factor()),CharacterScale.height(0,scale.factor()))).thenCompose(future->future),now+3_000_000_000L);
        }
        ScaleChange pending=target.scaleChange;if(pending==null)return;
        if(!pending.query().isDone()&&now<pending.deadline())return;target.scaleChange=null;
        Session requester=pending.requester();
        if(requester!=null&&(sessions.get(requester.address)!=requester||!playerRules.entitled(character(requester)))||!target.ready||target.hp<=0||target.respawning||target.teleport!=null)return;
        int result=0;try{if(pending.query().isDone())result=pending.query().join();}catch(Exception failure){result=0;}
        if(result!=1){lastScaleError="scale_placement_"+result;pending.query().cancel(false);enqueueOrRetire(requester==null?target:requester,GamePackets.chat(result==2?"Недостаточно места для такого размера.":"Место ещё загружается, повтори команду."));return;}
        if(requester==null){applyScale(target,pending.scale());return;}
        target.scaleSaving=true;
        inventories.scale(character(target),pending.scale(),()->{
            target.scaleSaving=false;if(sessions.get(target.address)!=target)return;target.configuredScale=pending.scale().factor();applyScale(target,pending.scale());
            if(sessions.get(requester.address)==requester)enqueueOrRetire(requester,GamePackets.chat("Размер "+target.grant.name()+": "+target.characterScale));
        },error->{target.scaleSaving=false;if(sessions.get(requester.address)==requester)enqueueOrRetire(requester,GamePackets.chat(error));});
    }
    private void applyScale(Session target,playerscale.ScaleValue scale){
        target.characterScale=scale.factor();enqueueOrRetire(target,scaledReady(target));for(Session viewer:List.copyOf(sessions.values()))if(viewer.ready)sendScale(viewer,target);
    }
    private void completion(Session s,byte[] body){long now=System.nanoTime();if(body.length!=1||body[0]!=1||s.lastCompletion!=0&&now-s.lastCompletion<250_000_000L)return;s.lastCompletion=now;completionRequests++;enqueue(s,0,CommandSupport.catalog(clones.catalog(),sessions.values().stream().filter(p->p.ready).map(p->p.grant.name()).distinct().sorted().toList()));}
    private Map<Integer,byte[]> hand(Session s){
        Map<Integer,byte[]> hand=new TreeMap<>();ItemStack item=inventory(s).at(new Inventory.Address(0,s.handContainer,0));boolean active=!s.holstered&&item!=null;
        hand.put(89,Wire.bytes(out->out.writeShort(s.handContainer==7?1:s.handContainer==5?2:s.handContainer==4?3:s.handContainer==10?4:0)));hand.put(91,new byte[]{(byte)(s.holstered?1:0)});hand.put(127,new byte[]{(byte)(active?1:0)});
        hand.put(124,active?Wire.bytes(out->{out.writeByte(0);out.writeByte(s.handContainer);out.writeShort(0);}):null);UUID identity=inventories.identity(character(s),s.handContainer);
        hand.put(125,active?Wire.bytes(out->{out.writeLong(identity.getMostSignificantBits());out.writeLong(identity.getLeastSignificantBits());}):null);hand.put(126,active?InventoryPackets.stack(item):null);return hand;
    }
    private boolean commandRuntime(Session s,String text){
        String verb=CommandSupport.verb(text);if(!Set.of("/clone","/anim","/apply","/help","/stats","/scale").contains(verb))return false;
        try{List<String> words=verb.equals("/apply")?CommandSupport.split(text):CommandSupport.words(text);
            switch(verb){
                case "/scale" -> {requestScale(s,CharacterScale.command(text));}
                case "/clone" -> {if(words.size()>2)throw new IllegalArgumentException("/clone [\"ник\"]");if(s.hp<=0||s.respawning||inventories.busy(character(s)))throw new IllegalArgumentException("clone_player_not_ready");
                    var state=new PlayerClones.Snapshot(s.pos,s.yaw,s.pitch,s.maxHp,equipment(s),hand(s),s.stats,animation(s),inventory(s).at(new Inventory.Address(0,1,0)),s.faction,s.animation.aiming(),s.posture.locomotion(),s.characterScale);
                    var clone=clones.create(character(s),words.size()==2?words.get(1):null,s.hp,state);enqueue(s,0,GamePackets.chat("Создан клон "+clone.name+" ("+clone.code+"). Здоровье: "+clone.health));}
                case "/anim" -> {if(words.size()==2){s.forcedAnimation=CommandSupport.normalizeAnimation(words.get(1));for(Session viewer:sessions.values())if(viewer.ready)pose(viewer,viewer==s?0:s.entity,effectivePose(s),true);}
                    else if(words.size()==4&&words.get(1).equalsIgnoreCase("clone")){var clone=clones.find(words.get(2));clone.animation=CommandSupport.normalizeAnimation(words.get(3));for(Session viewer:List.copyOf(clone.recipients))pose(viewer,clone.entity,clone.animation,true);}
                    else throw new IllegalArgumentException("/anim <анимация|clear> | clone <код|имя> <анимация|clear>");enqueue(s,0,GamePackets.chat("Анимация изменена."));}
                case "/apply" -> {if(!playerRules.entitled(character(s)))throw new IllegalArgumentException("apply_requires_admin");if(words.size()!=3)throw new IllegalArgumentException("/apply armor|skin|backpack|container|primary|secondary|melee <ID>");
                    PlayerCommands.Apply apply=PlayerCommands.apply(text);int container=apply.container(),factory=apply.factory();inventories.applyEquipment(character(s),container,factory,plan->inventoryChanged(s,plan,()->enqueue(s,0,GamePackets.chat("Экипировка изменена."))),error->enqueue(s,0,GamePackets.chat(error)));}
                case "/help" -> {if(words.size()!=1)throw new IllegalArgumentException("/help");enqueue(s,0,GamePackets.chat(CommandSupport.help()));}
                case "/stats" -> {if(words.size()!=1)throw new IllegalArgumentException("/stats");enqueue(s,0,GamePackets.chat(CommandSupport.artifactStats(inventories.artifactStats(character(s)))));}
            }
        }catch(IllegalArgumentException error){if(verb.equals("/scale"))lastScaleError=Objects.toString(error.getMessage(),"scale_failed");enqueue(s,0,GamePackets.chat(Objects.toString(error.getMessage(),"command_invalid")));}return true;
    }
    private void cloneInterest(Session viewer){
        int spawned=0;for(var clone:clones.all()){
            boolean visible=distance(viewer.pos,clone.state.position)<=PlayerClones.RANGE,known=clone.recipients.contains(viewer);
            if(!visible&&known){clone.recipients.remove(viewer);viewer.poses.remove(clone.entity);removeScale(viewer,clone.entity);enqueue(viewer,0,Wire.frame(2028,Wire.bytes(out->out.writeInt(clone.entity))));}
            else if(visible){if(!known){if(spawned++>=8)continue;sendCloneScale(viewer,clone);enqueue(viewer,0,Wire.concat(GamePackets.spawn(clone.entity,clone.name,clone.state.position,clone.state.yaw,clone.state.pitch),GamePackets.attributes(tick,clone.entity,clone.state.equipment),GamePackets.attributes(tick,clone.entity,Factions.attributes(clone.state.faction)),GamePackets.attributes(tick,clone.entity,clone.state.hand),GamePackets.vitals(tick,clone.entity,clone.health,clone.state.maximum,clone.state.aiming),GamePackets.movement(tick,clone.entity,clone.state.position,clone.state.yaw,clone.state.pitch,clone.state.movement)));clone.recipients.add(viewer);pose(viewer,clone.entity,clone.animation,true);}else pose(viewer,clone.entity,clone.animation,false);}
        }
    }
    private void damageClone(Session attacker,PlayerClones.Clone<Session> clone,float amount,double[] impact,double[] impulse,boolean head){damageClone(attacker,attacker.grant.name(),clone,amount,impact,impulse,head);}
    private void damageClone(Session attacker,String killerName,PlayerClones.Clone<Session> clone,float amount,double[] impact,double[] impulse,boolean head){damageClone(attacker,killerName,clone,amount,impact,impulse,head,new double[3]);}
    private void damageClone(Session attacker,String killerName,PlayerClones.Clone<Session> clone,float amount,double[] impact,double[] impulse,boolean head,double[] velocity){
        if(attacker!=null&&attacker.gameplayMode!=PlayerRules.Mode.STALKER&&!administrator(attacker)||!clones.live(clone)||clone.health<=0||!Float.isFinite(amount)||amount<=0)return;float applied=Math.min(clone.health,amount);clones.damage(clone,amount);cloneHits++;
        if(clone.health>0)for(Session viewer:List.copyOf(clone.recipients))if(sessions.get(viewer.address)==viewer)enqueueOrRetire(viewer,GamePackets.vitals(tick,clone.entity,clone.health,clone.state.maximum,clone.state.aiming));
        if(attacker!=null&&sessions.get(attacker.address)==attacker)enqueueOrRetire(attacker,Wire.dynamic("207064166",Wire.bytes(o->{o.writeBoolean(false);o.writeInt(head?4:3);o.writeBoolean(true);o.writeFloat(applied);o.writeFloat(0);o.writeBoolean(false);o.writeBoolean(false);o.writeBoolean(clone.health==0);o.writeBoolean(false);o.writeBoolean(false);o.writeFloat(0);o.writeFloat(0);o.writeInt(5);o.writeBoolean(false);o.writeBoolean(false);})));
        if(clone.health>0)return;cloneKills++;Set<Session> recipients=new HashSet<>(clone.recipients);if(attacker!=null&&sessions.get(attacker.address)==attacker)recipients.add(attacker);List<Session> viewers=recipients.stream().filter(v->sessions.get(v.address)==v&&v.ready).toList();var added=corpses.create(System.nanoTime(),viewers.stream().map(v->new Recipient(v.address,v.incarnation)).toList());corpseDespawn(added.evicted());
        byte[] corpse=GamePackets.corpse(added.corpse().entity(),clone.entity,clone.code,clone.state.position,clone.state.yaw,velocity,impact,impulse,clone.state.armor,null,clone.state.scale);
        for(Session viewer:viewers){viewer.poses.remove(clone.entity);removeScale(viewer,clone.entity);enqueueOrRetire(viewer,Wire.concat(GamePackets.movement(tick,clone.entity,clone.state.position,clone.state.yaw,clone.state.pitch,clone.state.movement),corpse,GamePackets.vitals(tick,clone.entity,0,clone.state.maximum,false),Wire.frame(2028,Wire.bytes(out->out.writeInt(clone.entity)))));}
        clones.remove(clone);if(attacker!=null&&sessions.get(attacker.address)==attacker)enqueueOrRetire(attacker,GamePackets.killNotification(clone.code));for(Session viewer:viewers)enqueueOrRetire(viewer,GamePackets.killFeed(clone.code,killerName));
    }
    private void receiveChat(Session s,byte[] body){
        try{chat(s,ChatPackets.request(body));}catch(IOException|IllegalArgumentException error){enqueue(s,0,GamePackets.chat(error.getMessage()));}
    }
    private void chat(Session s,String text){
        if(text.length()>16384)return;if(NpcEditorHooks.chat(this,s,text))return;if(NpcToolsHooks.chat(this,s,text))return;String verb=CommandSupport.verb(text);
        if(verb.equals("/zombies")){
            try{if(!playerRules.entitled(character(s)))throw new IllegalArgumentException("zombies_requires_admin");var words=CommandSupport.split(text);if(words.size()>2)throw new IllegalArgumentException("/zombies start|stop|status");String operation=words.size()==1?"start":words.get(1).toLowerCase(Locale.ROOT);String reply=switch(operation){case "start"->zombies.start(character(s));case "stop"->zombies.stop();case "status"->zombies.status().toString();default->throw new IllegalArgumentException("/zombies start|stop|status");};enqueue(s,0,GamePackets.chat(reply));}catch(IllegalArgumentException error){enqueue(s,0,GamePackets.chat(error.getMessage()));}return;
        }
        if(commandRuntime(s,text))return;
        if(verb.equals("/drone")){try{if(CommandSupport.split(text).size()!=1)throw new IllegalArgumentException("usage_drone");toggleFpv(s);}catch(IllegalArgumentException error){enqueueOrRetire(s,GamePackets.chat(error.getMessage()));}return;}
        if(ChunkDistanceCommands.matches(text)){try{int distance=ChunkDistanceCommands.parse(text),previous=s.window.radius();if(s.window.distance(distance)){s.windowEpoch++;pruneChunkWindow(s);}enqueue(s,0,szserver.client.ChunkDistancePackets.frame(distance));enqueue(s,0,GamePackets.chat("Дальность чанков: "+previous+" -> "+distance));}catch(IllegalArgumentException error){enqueue(s,0,GamePackets.chat(error.getMessage()));}return;}
        if(InventoryCommand.matches(text)){try{InventoryCommand request=InventoryCommand.parse(text);inventories.remove(character(s),request,s.pos.clone(),plan->inventoryChanged(s,plan,()->enqueue(s,0,GamePackets.chat(request.operation().equals("drop")?"Предмет выброшен ("+plan.moved()+" шт.)":"Предмет удалён ("+plan.moved()+" шт.)"))),error->enqueue(s,0,GamePackets.chat(error)));}catch(IllegalArgumentException error){enqueue(s,0,GamePackets.chat(error.getMessage()));}return;}
        if(EnvironmentCommands.matches(text)){try{if(!playerRules.entitled(character(s)))throw new IllegalArgumentException("environment_requires_admin");if(s.environmentSave!=null)throw new IllegalArgumentException("environment_command_pending");EnvironmentCommands.Request request=EnvironmentCommands.parse(text);s.weatherCommand=request.weather()!=null;s.environmentSave=environment.update(request);}catch(IllegalArgumentException error){enqueue(s,0,GamePackets.chat(error.getMessage()));}return;}
        String teleportVerb=verb;
        if(teleportVerb.equals("/tp")){try{teleport(s,TeleportCommands.parse(text),"Телепортация выполнена.");}catch(IllegalArgumentException error){enqueue(s,0,GamePackets.chat(error.getMessage()));}return;}
        if(teleportVerb.equals("/warp")){try{TeleportCommands.Warp request=TeleportCommands.warp(text);if(s.warpRead!=null||s.warpSave!=null)throw new IllegalArgumentException("warp_busy");if(request.operation().equals("teleport")){s.warpRead=warps.query(request.name());s.warpReadName=request.name();s.warpReadLife=s.life;}else{if(!playerRules.entitled(character(s)))throw new IllegalArgumentException("warp_requires_admin");if(s.warpSave!=null)throw new IllegalArgumentException("warp_busy");s.warpSave=request.operation().equals("delete")?warps.delete(request.name()):warps.set(request.name(),request.position());s.warpReply=request.operation().equals("delete")?"Варп "+request.name()+" удалён.":"Варп "+request.name()+" сохранён.";}}catch(IllegalArgumentException error){enqueue(s,0,GamePackets.chat(error.getMessage()));}return;}
        if(WorldEditChat.matches(text)){enqueue(s,0,GamePackets.chat(s.editor.accept(text,point(s.pos),playerRules.editor(character(s),worldEditors),commands)));return;}
        if(verb.equals("/spawn")){
            try{if(s.requestedSpawn!=null||s.spawnSave!=null)throw new IllegalArgumentException("Сохранение спавна уже выполняется.");if(s.hp<=0||s.respawning)throw new IllegalArgumentException("Спавн задаётся живым персонажем.");PlayerCommands.Spawn spawn=PlayerCommands.spawn(text);s.spawnScope=spawn.global()?null:character(s);if(spawn.global()&&!playerRules.entitled(character(s)))throw new IllegalArgumentException("global_spawn_requires_admin");World.Vec target=spawn.position()==null?point(s.pos):spawn.position();
                s.requestedSpawn=CharacterLocations.valid(target);s.spawnDeadline=System.nanoTime()+3_000_000_000L;s.nextSpawnCheck=0;
            }catch(IllegalArgumentException error){enqueue(s,0,GamePackets.chat(error.getMessage()));}return;
        }
        if(verb.equals("/fraction")){
            try{String faction=PlayerCommands.faction(text);inventories.faction(character(s),faction,()->{if(sessions.get(s.address)!=s)return;s.faction=inventories.faction(character(s));enqueue(s,0,GamePackets.attributes(++tick,0,Factions.attributes(s.faction)));for(Session other:sessions.values())if(other!=s&&other.ready)enqueue(other,0,GamePackets.attributes(tick,s.entity,Factions.attributes(s.faction)));enqueue(s,0,GamePackets.chat("Фракция изменена: "+s.faction));},error->enqueue(s,0,GamePackets.chat(error)));}catch(IllegalArgumentException error){enqueue(s,0,GamePackets.chat(error.getMessage()));}return;
        }
        if(verb.equals("/loadout")){
            try{PlayerCommands.Loadout request=PlayerCommands.loadout(text);if(request.operation().equals("list")){JsonObject progress=inventories.gameplay(character(s));String names=progress.has("loadouts")?String.join(", ",new TreeSet<>(progress.getAsJsonObject("loadouts").keySet())):"";enqueue(s,0,GamePackets.chat("Комплекты: "+names));return;}
                inventories.loadout(character(s),request.operation(),request.name(),plan->inventoryChanged(s,plan,()->enqueue(s,0,GamePackets.chat("Комплект: "+request.name()))),error->enqueue(s,0,GamePackets.chat(error)));}catch(IllegalArgumentException error){enqueue(s,0,GamePackets.chat(error.getMessage()));}return;
        }
        if(Set.of("/medkit","/medicine","/heal").contains(verb)){try{medicine(s,PlayerCommands.medicine(text));}catch(IllegalArgumentException error){enqueue(s,0,GamePackets.chat(error.getMessage()));}return;}
        if(Set.of("/gif","/give").contains(verb)||AccountIdentity.strip(text).startsWith("/{")){
            try{if(!playerRules.entitled(character(s)))throw new IllegalArgumentException("give_requires_admin");GiveCommand command=GiveCommand.parse(text);Session target=(command.target().equals("self")||command.target().equals("me")||command.target().equals(s.grant.name()))?s:sessions.values().stream().filter(p->p.ready&&p.grant.name().equals(command.target())).findFirst().orElseThrow(()->new IllegalArgumentException("give_target_offline"));
                inventories.give(character(target),command.factory(),command.count(),command.metadata(),command.tags(),plan->inventoryChanged(target,plan,()->enqueue(s,0,GamePackets.chat("Выдано: "+plan.moved()))),error->enqueue(s,0,GamePackets.chat(error)));
            }catch(RuntimeException error){enqueue(s,0,GamePackets.chat(Objects.toString(error.getMessage(),"give_failed")));}return;
        }
        if(Set.of("/storage","/stash").contains(verb)){try{var storage=PlayerCommands.storage(text);if(storage.operation().equals("open")){boolean opened=openStorage(s,0,true);enqueue(s,0,GamePackets.chat(opened?"Персональный склад открыт.":"Склад недоступен."));}else enqueue(s,0,GamePackets.chat("Склад перемещается штатным drag-and-drop в инвентаре."));}catch(IllegalArgumentException error){enqueue(s,0,GamePackets.chat(error.getMessage()));}return;}
        if(verb.startsWith("/")){enqueue(s,0,GamePackets.chat(ChatPackets.unknown(verb)));return;}
        byte[] message=ChatPackets.global(s.grant.name(),text,System.currentTimeMillis());for(Session other:new ArrayList<>(sessions.values()))if(other.ready)enqueueOrRetire(other,message);
    }
    private static double distance(double[] a,double[] b){double n=0;for(int i=0;i<3;i++)n+=(a[i]-b[i])*(a[i]-b[i]);return Math.sqrt(n);}
    private void nativeEcho(Session s,byte[] body)throws IOException{
        CreativeGiveHooks.echo(this,s,body);
        var echo=NativeAdminPackets.echo(body);byte[] raw=echo.stack();s.creativeHints.remove(echo.slot());
        if(raw==null)return;
        // This is a creative-view hint only. It never creates, replaces or edits a server item.
        for(var entry:inventory(s).items().entrySet())if(entry.getKey().aggregate()<2&&Arrays.equals(raw,InventoryPackets.stack(entry.getValue()))){s.creativeHints.put(echo.slot(),entry.getValue().id());nativeAdminEchoes++;return;}
    }
    private Session nativeGiveTarget(Session s,String requested){
        String name=requested.trim();if(name.isEmpty()||name.equals("self")||name.equals("me")||name.equals(s.grant.name()))return s;
        return sessions.values().stream().filter(p->p.ready&&p.grant.name().equals(name)).findFirst().orElseThrow(()->new IllegalArgumentException("give_target_offline"));
    }
    private void nativeGive(Session s,NativeAdminPackets.Give give){
        try{if(!playerRules.entitled(character(s)))throw new IllegalArgumentException("give_requires_admin");Session target=nativeGiveTarget(s,give.target());
            queueNativeGrant(s,new NativeGrant(target,give,null,null,s.life,System.nanoTime()+10_000_000_000L,give.config().length));
        }catch(IllegalArgumentException error){denyNativeGrant(s,error.getMessage());}
    }
    private void nativeAmmo(Session s,byte[] body)throws IOException{
        if(CreativeGiveHooks.ammo(this,s,body))return;
        if(body.length!=0)throw new IOException("admin_ammo_body");
        try{if(!playerRules.entitled(character(s)))throw new IllegalArgumentException("give_requires_admin");
            Inventory.Address selected=s.adminSelected;
            if(selected==null){Integer hint=s.creativeHints.get(s.creativeSelected);if(hint!=null)selected=inventory(s).items().entrySet().stream().filter(e->e.getKey().aggregate()<2&&e.getValue().id()==hint).map(Map.Entry::getKey).findFirst().orElse(null);}
            if(selected==null)selected=new Inventory.Address(0,s.handContainer,0);ItemStack weapon=inventory(s).at(selected);
            if(weapon==null||WeaponProfile.from(inventories.catalog().get(weapon.id()).properties())==null)throw new IllegalArgumentException("admin_ammo_weapon_missing");
            queueNativeGrant(s,new NativeGrant(s,null,selected,weapon,s.life,System.nanoTime()+10_000_000_000L,0));
        }catch(IllegalArgumentException error){denyNativeGrant(s,error.getMessage());}
    }
    private void queueNativeGrant(Session s,NativeGrant request){
        if(s.nativeGrants.size()>=32||s.nativeGrantBytes+request.bytes()>262144)throw new IllegalArgumentException("admin_give_queue_full");
        s.nativeGrants.addLast(request);s.nativeGrantBytes+=request.bytes();advanceNativeGrants(s);
    }
    private void denyNativeGrant(Session s,String error){nativeAdminDenied++;enqueueOrRetire(s,GamePackets.chat(error));}
    private void advanceNativeGrants(Session s){
        if(s.nativeGrantPending||s.nativeGrants.isEmpty())return;NativeGrant request=s.nativeGrants.peekFirst();Session target=request.target();
        String denial=null;if(!playerRules.entitled(character(s)))denial="give_requires_admin";
        else if(s.life!=request.life()||s.hp<=0||s.respawning||s.teleport!=null||System.nanoTime()>request.deadline())denial="admin_give_request_expired";
        else if(sessions.get(target.address)!=target||!target.ready)denial="give_target_offline";
        if(denial==null&&inventories.busy(character(target)))return;
        s.nativeGrants.removeFirst();s.nativeGrantBytes-=request.bytes();if(denial!=null){denyNativeGrant(s,denial);return;}
        s.nativeGrantPending=true;
        java.util.function.Consumer<Inventory.Plan> success=plan->{nativeAdminAccepted++;inventoryChanged(target,plan,()->{});s.nativeGrantPending=false;
            enqueueOrRetire(s,GamePackets.chat((request.give()!=null&&request.give().destination()==3?"admin_ground_archived: ":"admin_granted: ")+plan.moved()));
            if(target!=s&&request.give()!=null&&request.give().notifyTarget())enqueueOrRetire(target,GamePackets.chat("admin_received: "+plan.moved()));};
        java.util.function.Consumer<String> failure=error->{s.nativeGrantPending=false;denyNativeGrant(s,error);};
        if(request.give()!=null){inventories.grantConfig(character(target),request.give().config(),request.give().destination(),target.pos,success,failure);return;}
        ItemStack current=inventory(target).at(request.weapon());
        if(current==null||current.id()!=request.expected().id()){failure.accept("admin_ammo_weapon_changed");return;}
        try{WeaponProfile profile=WeaponProfile.from(inventories.catalog().get(current.id()).properties());int ammo=profile.ammunition().get(0);int count=inventories.catalog().get(ammo).stackLimit();inventories.give(character(target),ammo,count,0,new JsonObject(),success,failure);}
        catch(IllegalArgumentException error){failure.accept(error.getMessage());}
    }
    private boolean physicalActive(Session s){return s.physicalCycle!=null&&s.physicalCycle.identity().equals(s.firearm);}
    private void selectHand(Session s,szserver.client.HandPackets.Selection selection){
        var held=s.grenadeUse.held();if(held!=null&&held.mode()==0&&(selection.holstered()||selection.container()!=held.hand())){s.grenadeUse.cancel();grenadeUseCanceled++;}
        if(!s.weaponActions.hand(selection,s.life,System.nanoTime()))enqueue(s,0,GamePackets.chat("weapon_operation_queue_full"));
        advanceWeaponActions(s);
    }
    private void queueUnjam(Session s){if(!s.weaponActions.unjam(s.life,System.nanoTime()))enqueue(s,0,GamePackets.chat("weapon_operation_queue_full"));advanceWeaponActions(s);}
    private void startUnjam(Session s){
        if(s.weapon==null||!s.weapon.jamming().jammed()||s.hp<=0||s.holstered||s.respawning||s.teleport!=null||s.physicalPending||physicalActive(s)||inventories.busy(character(s))||s.unjamming!=null||s.unjammingPending||s.reloadAt!=0||s.launcherReload!=null){unjammingDenied++;return;}
        double duration=s.weapon.jamming().duration();if(!Double.isFinite(duration)||duration<=0||duration>120){unjammingDenied++;return;}
        s.unjamming=new Unjamming(System.nanoTime()+(long)(duration*1e9),s.life,s.handContainer,s.firearm,inventory(s).at(new Inventory.Address(0,s.handContainer,0)));unjammingStarted++;
    }
    private void cancelUnjam(Session s){if(s.unjamming!=null){s.unjamming=null;unjammingCanceled++;}}
    private void advanceUnjam(Session s,long now){
        Unjamming operation=s.unjamming;if(operation==null||s.unjammingPending)return;
        ItemStack current=inventory(s).at(new Inventory.Address(0,operation.hand(),0));
        if(s.hp<=0||s.respawning||s.teleport!=null||s.holstered||s.life!=operation.life()||s.handContainer!=operation.hand()||!Objects.equals(s.firearm,operation.identity())||current==null||!current.mergeable(operation.expected())||current.count()!=operation.expected().count()){cancelUnjam(s);return;}
        if(now<operation.deadline()||inventories.busy(character(s)))return;
        s.unjammingPending=true;s.unjamming=null;
        inventories.unjam(character(s),operation.hand(),operation.expected(),plan->{unjammingCompleted++;inventoryChanged(s,plan,()->{s.unjammingPending=false;});},error->{s.unjammingPending=false;unjammingDenied++;if(sessions.get(s.address)==s){syncAmmo(s);enqueue(s,0,GamePackets.chat(error));}});
    }
    private void queueReload(Session s,int ammo){
        if(!s.weaponActions.reload(ammo,s.life,System.nanoTime()))enqueue(s,0,GamePackets.chat("weapon_operation_queue_full"));
        advanceWeaponActions(s);
    }
    private void physicalRequest(Session s,byte[] body)throws IOException {
        var request=PhysicalReload.Request.read(body);
        if(!s.weaponActions.physical(request,s.life,System.nanoTime())){physicalDenied++;enqueue(s,0,GamePackets.chat("vr_operation_queue_full"));return;}
        advanceWeaponActions(s);
    }
    private void cancelReload(Session s){
        cancelUnjam(s);cancelLauncherReload(s);
        if(s.reloadAt!=0)enqueue(s,0,Wire.dynamic("365014982",new byte[0]));s.reloadAt=0;s.swing=null;s.animation.cancel();
        for(Session viewer:sessions.values())if(viewer.ready)enqueue(viewer,0,GamePackets.attributes(tick,viewer==s?0:s.entity,Map.of(201,new byte[]{0})));
    }
    private void advanceWeaponActions(Session s){
        if(s.physicalPending||s.unjammingPending||s.weaponActions.isEmpty()||inventories.busy(character(s)))return;
        long now=System.nanoTime();var next=s.weaponActions.peek(s.life,now);if(next==null)return;
        // The client's reload animation may have ended before this owner tick.
        // Finish its accepted magazine write before consuming the first firing request.
        if(next.kind()==WeaponActions.Kind.SHOT&&s.hp>0&&!s.respawning&&s.teleport==null&&s.reloadAt!=0&&now>=s.reloadAt){finishReload(s);return;}
        var action=s.weaponActions.poll(s.life,now);if(action==null)return;
        if(s.hp<=0||s.respawning||s.teleport!=null){if(action.kind()==WeaponActions.Kind.PHYSICAL)physicalDenied++;return;}
        if(action.kind()==WeaponActions.Kind.HAND){
            var selection=action.hand();
            if(selection.holstered()){cancelReload(s);s.holstered=true;syncHand(s);return;}
            int container=selection.container();
            if(inventory(s).at(new Inventory.Address(0,container,0))==null)return;
            if(container==s.handContainer&&!s.holstered){syncHand(s);return;}
            cancelReload(s);s.handContainer=container;s.holstered=false;refreshWeapon(s,()->{s.drawUntil=System.nanoTime()+(s.weapon==null?0:(long)(drawSeconds(s.weapon.factory())*1e9));syncHand(s);syncAmmo(s);});return;
        }
        if(action.kind()==WeaponActions.Kind.RELOAD){reload(s,action.ammo());return;}if(action.kind()==WeaponActions.Kind.LAUNCHER_RELOAD){reloadLauncher(s,action.ammo());return;}
        if(action.kind()==WeaponActions.Kind.UNJAM){startUnjam(s);return;}
        if(action.kind()==WeaponActions.Kind.SHOT){performShot(s,action.shot());return;}
        cancelUnjam(s);
        var request=action.physical();
        if(s.holstered||s.weapon==null||inventories.physicalMechanism(s.weapon.factory())==null){physicalDenied++;enqueue(s,0,GamePackets.chat("vr_supported_weapon_not_equipped"));return;}
        if(request.equals(s.physicalLast)&&Objects.equals(s.physicalIdentity,s.firearm)&&s.physicalReply!=null){physicalDuplicates++;enqueue(s,0,s.physicalReply);return;}
        s.physicalPending=true;s.reloadAt=0;s.animation.cancel();long life=s.life;UUID firearm=s.firearm;int container=s.handContainer;
        inventories.physicalReload(character(s),container,firearm,request,s.physicalCycle,result->{
            if(sessions.get(s.address)!=s)return;
            Runnable finish=()->{s.physicalPending=false;if(s.hp<=0||s.life!=life||s.handContainer!=container||!Objects.equals(s.firearm,firearm))return;
                var rule=result.result();s.physicalCycle=rule.disabled()?null:rule.cycle();s.physicalLast=request;s.physicalIdentity=firearm;s.physicalReply=rule.reply(request);if(rule.accepted())physicalAccepted++;else physicalDenied++;enqueue(s,0,s.physicalReply);};
            inventoryChanged(s,result.plan(),finish);
        },error->{s.physicalPending=false;physicalDenied++;if(sessions.get(s.address)==s)enqueue(s,0,GamePackets.chat(error));});
    }
    private double drawSeconds(int factory){var p=inventories.catalog().get(factory).properties();double ticks=p.has("draw_time")?p.get("draw_time").getAsDouble():10;return Double.isFinite(ticks)?Math.max(0,Math.min(2400,ticks))/20:.5;}
    private record ShotTarget(Session player,long life,PlayerClones.Clone<Session> copy,World.View bot,int entity){}
    private record AcceptedShot(ShotPackets.Shot packet,GameInventory.Weapon weapon,Map<String,Float> stats,JsonObject ammo,ShotTarget target,long life,int hand,UUID firearm,PlayerRules.Mode mode){}
    private ShotTarget reportedTarget(Session shooter,ShotPackets.Report report){
        if(report==null||report.entity()<0)return null;
        for(Session p:sessions.values())if(p!=shooter&&p.ready&&p.hp>0&&p.gameplayMode==PlayerRules.Mode.STALKER&&p.entity==report.entity()&&shooter.gameplayMode==PlayerRules.Mode.STALKER)return new ShotTarget(p,p.life,null,null,p.entity);
        if(shooter.gameplayMode==PlayerRules.Mode.STALKER||administrator(shooter))for(var c:clones.all())if(c.health>0&&c.recipients.contains(shooter)&&c.entity==report.entity())return new ShotTarget(null,0,c,null,c.entity);
        if(shooter.gameplayMode==PlayerRules.Mode.STALKER)for(var bot:world.snapshot().entities())if(bot.health()>0&&shooter.bots.contains(botKey(bot))&&Objects.equals(botEntities.get(botKey(bot)),report.entity()))return new ShotTarget(null,0,null,bot,report.entity());
        return null;
    }
    private boolean primaryLauncher(Session s){return s.weapon!=null&&inventories.explosives().containsKey(s.weapon.ammo());}
    private void queueLauncherReload(Session s,int ammo){
        if(!s.weaponActions.launcherReload(ammo,s.life,System.nanoTime()))enqueue(s,0,GamePackets.chat("weapon_operation_queue_full"));
        advanceWeaponActions(s);
    }
    private void cancelLauncherReload(Session s){
        if(s.launcherReload==null)return;s.launcherReload=null;underbarrelReloadCanceled++;s.animation.cancel();enqueue(s,0,UnderbarrelPackets.finished());
    }
    private void reloadLauncher(Session s,int requested){
        if(s.weapon==null||s.weapon.launcher()==null||s.hp<=0||s.holstered||s.respawning||s.teleport!=null||s.physicalPending||physicalActive(s)||s.launcherReload!=null||s.unjamming!=null||s.unjammingPending||s.reloadAt!=0||inventories.busy(character(s)))return;
        var launcher=s.weapon.launcher();if(launcher.rounds()!=0)return;
        int selected=requested;
        if(selected==0){
            boolean remembered=launcher.ammunition().contains(launcher.remembered())&&(administrator(s)||inventory(s).items().entrySet().stream().anyMatch(e->e.getKey().aggregate()==1&&e.getValue().id()==launcher.remembered()));
            selected=remembered?launcher.remembered():administrator(s)?launcher.ammunition().get(0):inventory(s).items().entrySet().stream().filter(e->e.getKey().aggregate()==1&&launcher.ammunition().contains(e.getValue().id())).mapToInt(e->e.getValue().id()).findFirst().orElse(0);
        }
        final int ammo=selected;if(!launcher.ammunition().contains(ammo)||!administrator(s)&&inventory(s).items().entrySet().stream().noneMatch(e->e.getKey().aggregate()==1&&e.getValue().id()==ammo))return;
        ItemStack expected=inventory(s).at(new Inventory.Address(0,s.handContainer,0));
        s.launcherReload=new LauncherReload(System.nanoTime()+(long)(launcher.reload()*1e9),s.life,s.handContainer,s.firearm,launcher.identity(),ammo,expected);s.animation.reload((float)launcher.reload());
        for(Session viewer:sessions.values())if(viewer.ready)enqueue(viewer,0,UnderbarrelPackets.reload(viewer==s?0:s.entity,launcher.reload(),ammo));
    }
    private void advanceLauncherReload(Session s,long now){
        LauncherReload pending=s.launcherReload;if(pending==null)return;
        if(s.hp<=0||s.holstered||s.respawning||s.teleport!=null||s.life!=pending.life()||s.handContainer!=pending.hand()||!Objects.equals(s.firearm,pending.firearm())||inventory(s).at(new Inventory.Address(0,pending.hand(),0))!=pending.expected()){cancelLauncherReload(s);return;}
        if(now<pending.deadline()||inventories.busy(character(s))||grenadeBridge.busy(character(s)))return;
        s.launcherReload=null;
        inventories.reloadLauncher(character(s),pending.hand(),pending.expected(),pending.launcher(),pending.ammo(),administrator(s),plan->{underbarrelReloads++;if(sessions.get(s.address)==s){enqueue(s,0,UnderbarrelPackets.finished());inventoryChanged(s,plan,()->{});}},error->{underbarrelDenied++;if(sessions.get(s.address)==s){enqueue(s,0,UnderbarrelPackets.finished());syncAmmo(s);enqueue(s,0,GamePackets.chat(error));}});
    }
    private void unloadLauncher(Session s,UnderbarrelPackets.Unload request){
        int owner=request.owner();var address=request.address();Runnable ack=()->enqueue(s,0,InventoryPackets.ack(owner,request.sequence()));
        java.util.function.Consumer<String> reject=error->{inventorySnapshot(s,owner);ack.run();enqueue(s,0,GamePackets.chat(error));};
        if(s.hp<=0||s.respawning||s.teleport!=null||s.reloadAt!=0||s.launcherReload!=null||s.unjamming!=null||s.unjammingPending||physicalActive(s)||grenadeBridge.busy(character(s))||s.grenadeUse.held()!=null){reject.accept("launcher_unload_busy");return;}
        if(owner==0&&address.aggregate()==2||owner==-1&&(!s.inventoryScreen.storageOpen()||address.aggregate()==0||address.aggregate()==2&&address.container()!=s.inventoryScreen.page())){reject.accept("inventory_owner");return;}
        inventories.unloadLauncher(character(s),address,plan->inventoryChanged(s,plan,ack),reject);
    }
    private void launchUnderbarrel(Session s){
        long now=System.nanoTime();
        if(s.weapon==null||s.weapon.launcher()==null||s.hp<=0||s.holstered||s.respawning||s.teleport!=null||s.physicalPending||s.unjamming!=null||s.unjammingPending||!s.weaponActions.isEmpty()||inventories.busy(character(s))||grenadeBridge.busy(character(s))||s.reloadAt!=0||s.launcherReload!=null||now<s.drawUntil||now<s.nextAttack||drones.owned(s)!=null&&drones.owned(s).piloting())return;
        var launcher=s.weapon.launcher();if(launcher.rounds()!=1)return;int ammo=launcher.ammo();var profile=grenadeProfiles.get(ammo);
        if(profile==null){underbarrelDenied++;syncAmmo(s);enqueue(s,0,GamePackets.chat("launcher_ammunition_unavailable:"+ammo));return;}
        Inventory.Address address=new Inventory.Address(0,s.handContainer,0);ItemStack expected=inventory(s).at(address);if(expected==null||expected.id()!=s.weapon.factory()||s.weapon.launcherMagazines()==null)return;
        var origin=GrenadePackets.origin(CharacterScale.eye(s.pos,s.posture.locomotion(),s.characterScale),s.yaw,s.pitch);var launchVelocity=LauncherPackets.velocity(profile,s.yaw,s.pitch);var operation=new ThrownGrenade(s,character(s),s.grant.name(),areaActor(s),-1,profile);long life=s.life;
        s.nextAttack=now+(long)(launcher.cooldown()*1e9);s.lastShot=now;
        grenadeBridge.launchMagazine(character(s),s.grant.name(),address,expected,s.weapon.launcherMagazines()[s.rounds],ammo,profile.flight(),origin,launchVelocity,operation.context,id->{
            publishGrenadeLaunch(id,operation,ammo,origin,launchVelocity);underbarrelShots++;grenadeThrows++;shots++;
            if(sessions.get(s.address)==s){refreshWeapon(s,()->syncAmmo(s));if(s.life==life)for(Session viewer:sessions.values())if(viewer.ready&&viewer!=s)enqueue(viewer,0,LauncherPackets.fired(s.entity,60/Math.max(.001,launcher.cooldown())));}
        },error->{underbarrelDenied++;if(sessions.get(s.address)==s){refreshWeapon(s,()->syncAmmo(s));enqueue(s,0,GamePackets.chat(error));}});
    }
    private void launchPrimary(Session s){
        if(s.weapon!=null&&s.weapon.jamming().jammed()){primaryLauncherDenied++;return;}
        long now=System.nanoTime();
        if(!primaryLauncher(s)||s.hp<=0||s.holstered||s.respawning||s.teleport!=null||s.physicalPending||s.unjamming!=null||s.unjammingPending||!s.weaponActions.isEmpty()||inventories.busy(character(s))||grenadeBridge.busy(character(s))||s.rounds<=0||s.reloadAt!=0||s.launcherReload!=null||now<s.drawUntil||now<s.nextAttack||drones.owned(s)!=null&&drones.owned(s).piloting())return;
        int ammo=s.weapon.ammo();var profile=grenadeProfiles.get(ammo);
        if(profile==null){primaryLauncherDenied++;syncAmmo(s);enqueue(s,0,GamePackets.chat("launcher_ammunition_unavailable:"+ammo));return;}
        Inventory.Address address=new Inventory.Address(0,s.handContainer,0);ItemStack expected=inventory(s).at(address);
        if(expected==null||expected.id()!=s.weapon.factory()||s.magazines==null||s.rounds>=s.magazines.length)return;
        int remaining=s.rounds-1;long life=s.life;UUID firearm=s.firearm;double rpm=s.weapon.profile().rpm();
        var origin=GrenadePackets.origin(CharacterScale.eye(s.pos,s.posture.locomotion(),s.characterScale),s.yaw,s.pitch);var launchVelocity=LauncherPackets.velocity(profile,s.yaw,s.pitch);
        var operation=new ThrownGrenade(s,character(s),s.grant.name(),areaActor(s),-1,profile);
        s.nextAttack=now+(long)(60/rpm*1e9);s.lastShot=now;
        grenadeBridge.launchMagazine(character(s),s.grant.name(),address,expected,s.magazines[remaining],ammo,profile.flight(),origin,launchVelocity,operation.context,id->{
            publishGrenadeLaunch(id,operation,ammo,origin,launchVelocity);primaryLauncherShots++;grenadeThrows++;shots++;
            if(sessions.get(s.address)==s){if(s.life==life&&s.handContainer==address.container()&&Objects.equals(s.firearm,firearm)){s.rounds=remaining;syncAmmo(s);}else refreshWeapon(s,()->syncAmmo(s));}
            if(sessions.get(s.address)==s&&s.life==life)for(Session viewer:sessions.values())if(viewer.ready&&viewer!=s)enqueue(viewer,0,LauncherPackets.fired(s.entity,rpm));
        },error->{primaryLauncherDenied++;if(sessions.get(s.address)==s){refreshWeapon(s,()->syncAmmo(s));enqueue(s,0,GamePackets.chat(error));}});
    }
    private void shot(Session shooter,byte[] raw)throws Exception{
        if(primaryLauncher(shooter)||vehicles.driving(shooter)!=null&&!vehicleActions.windowFire(shooter,System.nanoTime()))return;
        var packet=ShotPackets.read(raw);
        if(!shooter.weaponActions.shot(packet,raw.length,shooter.life,System.nanoTime())){enqueue(shooter,0,GamePackets.chat("weapon_operation_queue_full"));return;}
        advanceWeaponActions(shooter);
    }
    private void performShot(Session shooter,ShotPackets.Shot packet){
        if(primaryLauncher(shooter)||vehicles.driving(shooter)!=null&&!vehicleActions.windowFire(shooter,System.nanoTime()))return;
        var shotKey=new ShotKey(packet.seed(),packet.sequence());long now=System.nanoTime();
        shooter.seenShots.values().removeIf(t->now-t>=30_000_000_000L);
        if(shieldShots.size()>=32||shooter.seenShots.containsKey(shotKey)||shooter.hp<=0||shooter.holstered||shooter.weapon==null||shooter.weapon.jamming().jammed()||shooter.unjamming!=null||shooter.unjammingPending||shooter.physicalPending||physicalActive(shooter)&&!shooter.physicalCycle.canFire()||inventories.busy(character(shooter))||(!administrator(shooter)&&shooter.rounds==0)||shooter.reloadAt!=0||shooter.launcherReload!=null||now<shooter.drawUntil||!ShotTiming.allowed(now,shooter.nextAttack,shooter.lastShotClientTick,packet.serverTick(),shooter.weapon.profile().rpm()))return;
        if(shooter.seenShots.size()>=4096)shooter.seenShots.remove(shooter.seenShots.keySet().iterator().next());shooter.seenShots.put(shotKey,now);
        var accepted=new AcceptedShot(packet,shooter.weapon,shooter.stats,inventories.catalog().get(shooter.weapon.ammo()).properties().deepCopy(),reportedTarget(shooter,packet.report()),shooter.life,shooter.handContainer,shooter.firearm,shooter.gameplayMode);
        shooter.sequence=packet.sequence();shooter.lastShot=now;shooter.nextAttack=ShotTiming.next(now,shooter.nextAttack,shooter.lastShotClientTick,shooter.weapon.profile().rpm());shooter.lastShotClientTick=packet.serverTick();
        if(administrator(shooter)){shots++;syncAmmo(shooter);finishShot(shooter,accepted);return;}
        PhysicalReload.Cycle cycle=physicalActive(shooter)?shooter.physicalCycle.shot(inventories.physicalMechanism(shooter.weapon.factory())):null;
        JsonObject progress=PhysicalReload.progress(inventories.gameplay(character(shooter)),accepted.firearm(),cycle);int rounds=shooter.rounds-1;byte[] magazine=shooter.magazines[rounds];Inventory.Address address=new Inventory.Address(0,accepted.hand(),0);ItemStack expected=inventory(shooter).at(address);
        inventories.mutate(character(shooter),before->{if(!Objects.equals(before.at(address),expected))throw new IllegalArgumentException("shot_weapon_changed");return before.replace(Map.of(address,expected.state(magazine)),1);},progress,plan->{
            shots++;if(sessions.get(shooter.address)!=shooter)return;
            if(shooter.life!=accepted.life()||shooter.handContainer!=accepted.hand()||!Objects.equals(shooter.firearm,accepted.firearm())){refreshWeapon(shooter,()->syncAmmo(shooter));return;}
            shooter.rounds=rounds;shooter.physicalCycle=cycle;syncAmmo(shooter);if(shooter.hp>0&&!shooter.holstered&&shooter.gameplayMode==accepted.mode())finishShot(shooter,accepted);
        },error->{if(sessions.get(shooter.address)==shooter){syncAmmo(shooter);enqueue(shooter,0,GamePackets.chat(error));}});
    }
    private void finishShot(Session shooter,AcceptedShot accepted){finishShot(shooter,accepted,true);}
    private void finishShot(Session shooter,AcceptedShot accepted,boolean checkShield){
        var shot=accepted.packet();var profile=accepted.weapon().profile();double[] origin=shot.origin().vector(),end=shot.tracer(profile.range()),dir=shot.direction();var report=shot.report();ShotTarget target=accepted.target();
        boolean live=target!=null&&(target.player()!=null?sessions.get(target.player().address)==target.player()&&target.player().life==target.life()&&target.player().hp>0&&target.player().gameplayMode==PlayerRules.Mode.STALKER:target.copy()!=null?clones.live(target.copy())&&target.copy().health>0:world.snapshot().entities().stream().anyMatch(v->botKey(v).equals(botKey(target.bot()))&&v.health()>0));
        double range=report==null?0:shot.origin().distance(report.impact());String stage=report==null?"missing_client_update_required":!live?"miss":range>profile.range()+2?"outside_weapon_range":"hit";
        boolean hit=stage.equals("hit");if(hit||report!=null&&report.entity()==-1)end=report.impact().vector();final double[] impact=end;
        if(checkShield){var barrier=grenadeAreaScenes.shieldContact(grenadeAreas.states(),origin,end,System.currentTimeMillis());if(barrier!=null){float amount=(float)profile.bulletDamage(barrier.distance(),false,accepted.stats(),Map.of(),accepted.ammo());if(amount>0){UUID request=UUID.nameUUIDFromBytes((character(shooter)+":"+accepted.firearm()+":"+shot.seed()+":"+shot.sequence()).getBytes(java.nio.charset.StandardCharsets.UTF_8));long admitted=System.currentTimeMillis();shieldShots.addLast(new ShieldShot(shooter,accepted,barrier,amount,admitted,request,grenadeAreas.hitShield(barrier.activation().projectile(),request,amount,admitted)));return;}}}
        lastShot=Map.of("stage",stage,"authority","client","origin",origin,"end",end,"target",hit?target.entity():-1,"nativeEntity",report==null?-2:report.entity(),"nativeGroup",report==null?0:report.group());
        for(Session viewer:sessions.values())if(viewer.ready){int entity=viewer==shooter?0:shooter.entity;
            if(viewer!=shooter)enqueue(viewer,0,Wire.dynamic("1176115659",Wire.bytes(out->{out.writeInt(entity);out.writeBoolean(false);out.writeFloat((float)(60/profile.rpm()));})));
            Integer destination=hit?(viewer==target.player()?0:target.entity()):null;enqueue(viewer,0,GamePackets.trajectory(entity,origin,impact,destination));
        }
        if(!hit)return;boolean head=report.group()==2;Map<String,Float> stats=target.player()!=null?target.player().stats:target.copy()!=null?target.copy().state.stats:world.snapshot().entities().stream().filter(b->botKey(b).equals(botKey(target.bot()))).map(World.View::stats).findFirst().orElse(Map.of());
        float amount=(float)profile.bulletDamage(range,head,accepted.stats(),stats,accepted.ammo());double[] impulse=GamePackets.bulletImpulse(dir,profile.damageAt(range));
        if(target.player()!=null){hits++;damage(shooter,target.player(),amount,origin,impact,impulse,head);}
        else if(target.copy()!=null){hits++;damageClone(shooter,target.copy(),amount,impact,impulse,head);}
        else damageBot(shooter,target.bot(),amount,head);
    }
    private void advanceShieldShots(){
        while(!shieldShots.isEmpty()&&shieldShots.peekFirst().future().isDone()){
            var task=shieldShots.removeFirst();GrenadeAreaStore.ShieldHit result;
            try{result=task.future().join();}catch(CompletionException error){if(error.getCause() instanceof RejectedExecutionException){shieldShots.addFirst(new ShieldShot(task.shooter(),task.shot(),task.contact(),task.damage(),task.admitted(),task.request(),grenadeAreas.hitShield(task.contact().activation().projectile(),task.request(),task.damage(),task.admitted())));return;}shieldErrors++;continue;}
            if(!result.blocked()){if(!closed)finishShot(task.shooter(),task.shot(),true);continue;}
            if(result.applied())shieldBlocked++;if(closed||sessions.get(task.shooter().address)!=task.shooter()||task.shooter().life!=task.shot().life())continue;
            var packet=task.shot().packet();double[] origin=packet.origin().vector(),direction=packet.direction(),point={origin[0]+direction[0]*task.contact().distance(),origin[1]+direction[1]*task.contact().distance(),origin[2]+direction[2]*task.contact().distance()};
            lastShot=Map.of("stage","shield","authority","server-native-aabb","shield",task.contact().activation().projectile(),"remaining",result.after(),"origin",origin,"end",point);
            for(Session viewer:List.copyOf(sessions.values()))if(viewer.ready){int entity=viewer==task.shooter()?0:task.shooter().entity;if(viewer!=task.shooter())enqueue(viewer,0,Wire.dynamic("1176115659",Wire.bytes(out->{out.writeInt(entity);out.writeBoolean(false);out.writeFloat((float)(60/task.shot().weapon().profile().rpm()));})));enqueue(viewer,0,GamePackets.trajectory(entity,origin,point,null));}
        }
    }
    private static double[] WireDoubles(double[] a,double[] b){return new double[]{a[0],a[1],a[2],b[0],b[1],b[2]};}
    private static double rayBox(double[] origin,double[] dir,double[] lo,double[] hi,double max){double near=0,far=max;for(int i=0;i<3;i++){if(Math.abs(dir[i])<1e-9){if(origin[i]<lo[i]||origin[i]>hi[i])return -1;continue;}double a=(lo[i]-origin[i])/dir[i],b=(hi[i]-origin[i])/dir[i];near=Math.max(near,Math.min(a,b));far=Math.min(far,Math.max(a,b));if(near>far)return -1;}return near;}
    private void finishReload(Session s){
        boolean infinite=administrator(s);int count=infinite?s.weapon.capacity()-s.rounds:Math.min(s.weapon.capacity()-s.rounds,s.reserve);
        int ammo=s.weapon.ammo(),rounds=s.rounds+count,hand=s.handContainer;long life=s.life;UUID identity=s.firearm;
        Inventory.Address weapon=new Inventory.Address(0,hand,0);ItemStack expected=inventory(s).at(weapon);byte[] magazine=s.magazines[rounds];s.reloadAt=0;
        inventories.mutate(character(s),before->{
            if(!Objects.equals(before.at(weapon),expected))throw new IllegalArgumentException("reload_weapon_changed");
            Map<Inventory.Address,ItemStack> changes=new LinkedHashMap<>();int remaining=infinite?0:count;
            for(var e:before.items().entrySet())if(e.getKey().aggregate()==1&&e.getValue().id()==ammo&&remaining>0){int used=Math.min(remaining,e.getValue().count());changes.put(e.getKey(),e.getValue().count(e.getValue().count()-used));remaining-=used;}
            if(remaining!=0)throw new IllegalArgumentException("reload_ammunition_changed");changes.put(weapon,expected.state(magazine));return before.replace(changes,count);
        },plan->{
            if(closed||sessions.get(s.address)!=s)return;
            if(s.life!=life||s.handContainer!=hand||!Objects.equals(s.firearm,identity)){inventoryChanged(s,plan,()->{});return;}
            s.rounds=rounds;refreshReserve(s);enqueue(s,0,Wire.dynamic("365014982",new byte[0]));for(var e:plan.changes().entrySet())enqueue(s,0,InventoryPackets.point(0,-1,e.getKey(),e.getValue()));syncAmmo(s);
        },error->{if(!closed&&sessions.get(s.address)==s){enqueue(s,0,Wire.dynamic("365014982",new byte[0]));syncAmmo(s);enqueue(s,0,GamePackets.chat(error));}});
    }
    private void reload(Session s,int requested){
        if(s.weapon!=null&&s.weapon.jamming().jammed()){startUnjam(s);return;}
        lastReload=Map.of("name",s.grant.name(),"requested",requested,"rounds",s.rounds,"capacity",s.weapon==null?0:s.weapon.capacity(),"reserve",s.reserve,"holstered",s.holstered,"busy",inventories.busy(character(s)),"pending",s.reloadAt!=0);
        if(s.physicalPending||physicalActive(s)||s.weapon==null||s.hp<=0||s.holstered||inventories.busy(character(s))||s.rounds>=s.weapon.capacity()||s.reloadAt!=0||s.launcherReload!=null||s.unjamming!=null||s.unjammingPending)return;
        int selected=administrator(s)?(requested==0?(s.weapon.profile().ammunition().contains(s.weapon.ammo())?s.weapon.ammo():s.weapon.profile().ammunition().stream().sorted().findFirst().orElse(0)):requested):inventory(s).items().entrySet().stream().filter(e->e.getKey().aggregate()==1&&s.weapon.profile().ammunition().contains(e.getValue().id())&&(requested==0||e.getValue().id()==requested)&&(s.rounds==0||e.getValue().id()==s.weapon.ammo())).map(e->e.getValue().id()).findFirst().orElse(0);if(selected==0||!s.weapon.profile().ammunition().contains(selected)||s.rounds>0&&selected!=s.weapon.ammo())return;
        if(selected!=s.weapon.ammo()){inventories.weapon(character(s),s.handContainer,selected,weapon->{if(sessions.get(s.address)!=s)return;s.weapon=weapon;s.magazines=weapon.magazines();s.firearm=weapon.identity();s.rounds=weapon.rounds();refreshReserve(s);startReload(s);},error->enqueue(s,0,GamePackets.chat(error)));}else{refreshReserve(s);startReload(s);}
    }
    private void startReload(Session s){
        double duration=s.rounds==0?s.weapon.profile().reload():s.weapon.profile().tacticalReload();if(!Double.isFinite(duration)||duration<=0||duration>120)throw new IllegalArgumentException("reload_duration");s.reloadAt=System.nanoTime()+(long)(duration*1_000_000_000L);s.animation.reload((float)duration);
        enqueue(s,0,Wire.dynamic("990569665",Wire.bytes(o->{o.writeUTF("reload");o.writeFloat((float)duration);o.writeBoolean(s.rounds==0);o.writeBoolean(false);o.writeUTF("reload");})));
        for(Session viewer:sessions.values())if(viewer.ready){int entity=viewer==s?0:s.entity;enqueue(viewer,0,Wire.dynamic("1072250159",Wire.bytes(o->{o.writeInt(entity);o.writeFloat((float)(duration*.87));o.writeFloat((float)(duration*.96));o.writeFloat((float)duration);o.write(GamePackets.stack(s.weapon.ammo(),(administrator(s)?s.weapon.capacity()-s.rounds:Math.min(s.weapon.capacity()-s.rounds,s.reserve)),new byte[0]));o.writeBoolean(s.rounds==0);o.writeBoolean(s.rounds!=0);o.writeBoolean(false);o.writeInt(-2);o.writeShort(0);})) );}
    }
    private void loadout(Session s){vitals(s);enqueue(s,0,GamePackets.attributes(tick,0,equipment(s)));inventorySnapshot(s,0);syncHand(s);syncAmmo(s);}
    private void syncAmmo(Session s){if(s.weapon!=null)enqueue(s,0,InventoryPackets.point(0,-1,new Inventory.Address(0,s.handContainer,0),inventory(s).at(new Inventory.Address(0,s.handContainer,0))));enqueue(s,0,GamePackets.counter(s.rounds));}
    private void step(long now)throws Exception{tick++;advanceWorldItems();zombies.tick(sessions.values().stream().filter(s->s.ready).map(s->new ZombieHorde.Target(character(s),point(s.pos),Math.max(.3,CharacterScale.height(s.posture.locomotion(),s.characterScale)-.15),s.hp>0&&!s.respawning&&s.teleport==null)).toList());advanceHordeCombat(now);advanceVehicles(now);advanceGrenadeUses(now);pollGrenades();for(var change:drones.expire(now))emitFpv(change.drone(),change.event());for(var expired:corpses.expire(now))corpseDespawn(expired);environment.pollExpiry();var globalEnvironment=environment.state();if(environment.settingsRevision()!=environmentRevision||now>=nextEnvironmentClock){environmentRevision=environment.settingsRevision();nextEnvironmentClock=now+5_000_000_000L;byte[] clockFrame=EnvironmentPackets.clock(environment.clockAnchor(),System.currentTimeMillis());for(Session ready:sessions.values())if(ready.ready)enqueue(ready,0,clockFrame);}if(now>=nextLocationFlush){locations.flush();nextLocationFlush=now+1_000_000_000L;}simulationCorpses();for(Session s:new ArrayList<>(sessions.values())){if(sessions.get(s.address)!=s)continue;if(s.ready){String editorReply=s.editor.poll();if(editorReply!=null)enqueue(s,0,GamePackets.chat(editorReply));}if(s.environmentSave!=null&&s.environmentSave.isDone()){try{var saved=s.environmentSave.join();enqueue(s,0,GamePackets.chat(s.weatherCommand?"Глобальная погода изменена: "+saved.weather().wireName():"Глобальное время изменено: "+saved.dayTime()+" ticks."));}catch(CompletionException failure){enqueue(s,0,GamePackets.chat("environment_save_failed"));}s.environmentSave=null;}if(s.authentication!=null&&s.authentication.isDone()){
            try{LocalAccounts.Grant grant=s.authentication.join();s.authentication=null;beginWorldJoin(s,grant,s.loginProtocol);}catch(Exception error){rejected++;remove(s);continue;}
        }if(sessions.get(s.address)!=s)continue;if(s.accountPreparation!=null&&s.accountPreparation.isDone()){
            try{if(!s.accountPreparation.join())throw new IllegalArgumentException("character_deleted_or_deleting");s.accountPreparation=null;beginInventoryWorldJoin(s);}catch(Exception error){rejected++;remove(s);continue;}
        }if(s.locationLease!=null&&s.locationProtocol!=null&&s.locationLease.isDone()&&s.inventoryPreparation!=null&&s.inventoryPreparation.isDone()){
            try{s.inventoryPreparation.join();s.inventoryPreparation=null;s.locationLease.join();UUID protocol=s.locationProtocol;s.locationProtocol=null;beginPreparedWorldJoin(s,s.grant,protocol);}catch(Exception error){rejected++;remove(s);continue;}
        }if(s.grant!=null)for(var emission:environmentAccess.poll(s.environmentClient,playerRules.editor(character(s),worldEditors),tick,s.lanes[1].queued.size()<16&&s.lanes[1].sent.size()<32)){enqueue(s,emission.lane(),emission.bytes());}advanceScale(s,now);advanceRespawnState(s);advancePlacement(s,now);if(!s.ready&&s.inventoryReady&&s.positioned)completeJoin(s);advanceSpawn(s,now);advanceSceneConfiguration(s);advancePortal(s,now);advanceTeleport(s,now);if(sessions.get(s.address)!=s)continue;
        if(s.warpRead!=null&&s.warpRead.isDone()){
            var read=s.warpRead;s.warpRead=null;
            try{World.Vec target=read.join();if(s.life!=s.warpReadLife||s.hp<=0||s.respawning||s.teleport!=null)throw new IllegalArgumentException("warp_request_expired");if(target==null)throw new IllegalArgumentException("warp_not_found");teleport(s,new TeleportCommands.Request(target,null,false),"\u0422\u0435\u043b\u0435\u043f\u043e\u0440\u0442\u0430\u0446\u0438\u044f \u043d\u0430 \u0432\u0430\u0440\u043f "+s.warpReadName);}
            catch(Exception error){Throwable cause=error instanceof CompletionException&&error.getCause()!=null?error.getCause():error;enqueue(s,0,GamePackets.chat(cause.getMessage()==null?"warp_read_failed":cause.getMessage()));}
        }
        if(s.warpSave!=null&&s.warpSave.isDone()){try{s.warpSave.join();enqueue(s,0,GamePackets.chat(s.warpReply));}catch(Exception error){enqueue(s,0,GamePackets.chat("warp_save_failed"));}s.warpSave=null;}if(s.spawnSave!=null&&s.spawnSave.isDone()){try{s.spawnSave.join();enqueue(s,0,GamePackets.chat("Точка спавна сохранена."));}catch(Exception failed){enqueue(s,0,GamePackets.chat("Не удалось сохранить точку спавна."));}s.spawnSave=null;}
        if(now-s.last>(s.ready?30_000_000_000L:15_000_000_000L)){remove(s);continue;}try{
            if(s.ready){advanceNativeGrants(s);advanceWeaponActions(s);if(s.hp<=0&&s.deadUntil!=0&&now-s.deadUntil>=0&&!s.respawning&&!inventories.busy(character(s))&&!vitalsPending(character(s))){if(s.teleport!=null)s.teleport.respawnRequested=true;else beginRespawn(s);}streamChunks(s);bots(s);cloneInterest(s);playerPoses(s);if(!s.respawning){effectTick(s,now);staminaTick(s,now);}advanceUnjam(s,now);if(s.reloadAt!=0&&now>=s.reloadAt&&!inventories.busy(character(s)))finishReload(s);advanceLauncherReload(s,now);enqueue(s,0,packets.tick(tick,s.clientTick,movementConfiguration(s)));}if(s.ready&&!s.respawning&&locations.healthy())observePlayer(s);
        }catch(IllegalStateException e){remove(s);}}
        anomaliesTick();
        // Complete session mutations before capturing each player visual state once.
        Map<UUID,szserver.world.World.Vec> targets=new HashMap<>();for(Session s:sessions.values())if(s.ready&&s.hp>0&&!s.respawning&&s.teleport==null)targets.put(character(s),point(s.pos));world.publishPlayers(targets);
        Map<UUID,szserver.world.ActorMinePlayer> pressure=new HashMap<>();for(Session s:sessions.values())if(s.ready&&s.hp>0&&!s.respawning&&s.teleport==null&&s.pos[1]>=0&&s.pos[1]<=255)pressure.put(character(s),new szserver.world.ActorMinePlayer(character(s),s.incarnation,s.life,point(s.pos),s.stats,true,s.gameplayMode!=PlayerRules.Mode.STALKER,s.posture.locomotion()));world.publishMinePlayers(pressure);pollMines();
        List<PlayerReplication.State> playerStates=sessions.values().stream().filter(s->s.ready).map(s->new PlayerReplication.State(s.entity,s.pos,s.yaw,s.pitch,animation(s))).toList();
        for(Session s:new ArrayList<>(sessions.values())){
            if(sessions.get(s.address)!=s)continue;
            try{
                if(s.ready){
                    Lane output=s.lanes[0];
                    if(output.queued.size()>32||output.sent.size()>=WINDOW){replicationDeferred++;}
                    else{
                        var delivery=s.replication.prepare(tick,s.entity,playerStates);
                        if(delivery!=null){int fragments=(delivery.frame().length+FRAGMENT-1)/FRAGMENT;
                            if(output.queued.size()+fragments<=MAX_QUEUE){enqueue(s,0,delivery.frame());delivery.accepted().run();replicationFrames++;replicationRecords+=delivery.records();replicationBytes+=delivery.frame().length;}
                            else replicationDeferred++;
                        }
                    }
                }
            }catch(IllegalStateException failure){remove(s);}
        }
        // A failed flush can retire a source and enqueue its despawn. All movement
        // frames must precede that despawn, including frames for later viewers.
        for(Session s:new ArrayList<>(sessions.values())){
            if(sessions.get(s.address)!=s)continue;
            try{flush(s,now);}catch(IOException failure){remove(s);}
        }
        Set<BotKey> aliveBots=new HashSet<>();world.snapshot().entities().forEach(bot->aliveBots.add(botKey(bot)));botEntities.entrySet().removeIf(entry->{if(aliveBots.contains(entry.getKey()))return false;entityIds.release(entry.getValue());return true;});advancePlayerStates();pruneCharacterSessions(Long.MAX_VALUE);var m=new LinkedHashMap<String,Object>();m.put("worldItems",Map.of("simulation",worldItems.metrics(),"dropped",itemDropped,"collected",itemCollected,"failures",itemFailures,"requests",itemRequests,"lastError",lastItemFailure));m.put("characterSessions",characterSessions.metrics());m.put("replicatedBots",botEntities.size());m.put("anomalies",Map.of("simulation",anomalies.metrics(),"queryErrors",anomalyErrors,"queryPending",anomalyQuery!=null,"velocityAcknowledgements",anomalyVelocityEchoes));m.put("mines",Map.of("explosions",mineExplosions,"hits",mineHits,"pollErrors",minePollErrors));m.put("playerReplication",Map.of("frames",replicationFrames,"records",replicationRecords,"bytes",replicationBytes,"deferred",replicationDeferred,"baselines",sessions.values().stream().mapToInt(s->s.replication.retained()).sum(),"maxRecordsPerFrame",64));m.put("physicalReload",Map.of("accepted",physicalAccepted,"denied",physicalDenied,"duplicates",physicalDuplicates,"queued",sessions.values().stream().mapToLong(s->s.weaponActions.physicalCount()).sum(),"active",sessions.values().stream().filter(this::physicalActive).count()));m.put("weaponActions",Map.of("queued",sessions.values().stream().mapToInt(s->s.weaponActions.size()).sum(),"rejected",sessions.values().stream().mapToLong(s->s.weaponActions.rejected()).sum(),"expired",sessions.values().stream().mapToLong(s->s.weaponActions.expired()).sum()));m.put("commandRuntime",Map.of("completionRequests",completionRequests,"poseRevision",poseRevision,"clones",clones.metrics(),"cloneHits",cloneHits,"cloneKills",cloneKills));m.put("fpv",Map.of("lifecycle",drones.metrics(),"denied",fpvDenied,"explosions",fpvExplosions,"capable",sessions.values().stream().filter(s->s.fpvCapable).count(),"observers",droneObservers.size(),"placed",fpvPlaced,"pendingPlacements",fpvPlacements.size(),"geometry",fpvMap.metrics(),"receipts",fpvReceipts.metrics()));m.put("grenades",Map.of("bridge",grenadeBridge.metrics(),"profiles",grenadeProfiles.size(),"unavailable",Map.copyOf(grenadeUnavailable),"active",thrownGrenades.size(),"throws",grenadeThrows,"denied",grenadeDenied,"explosions",grenadeExplosions,"hits",grenadeHits,"pollErrors",grenadePollErrors,"outbox",grenadeOutbox.metrics()));m.put("grenadeAreaPreparation",Map.of("items",grenadeAreaProfiles.size(),"profiles",grenadeAreaProfiles.values().stream().mapToInt(List::size).sum(),"gameplayIntegrated",true,"backendIntegrated",true));m.put("grenadeAreaScenes",Map.of("service",grenadeAreaScenes.metrics(),"enabledSmokeItems",enabledAreaProfiles.keySet().stream().filter(item->grenadeUseRules.get(item).handUse()&&GrenadeAreaScenes.smoke(enabledAreaProfiles.get(item))).count(),"enabledGasItems",enabledAreaProfiles.keySet().stream().filter(item->grenadeUseRules.get(item).handUse()&&GrenadeAreaScenes.gas(enabledAreaProfiles.get(item))).count(),"active",areaScenes.size(),"spawns",areaSceneSpawns,"removals",areaSceneRemovals,"enabledStatusItems",enabledAreaProfiles.keySet().stream().filter(item->grenadeUseRules.get(item).handUse()&&GrenadeAreaScenes.status(enabledAreaProfiles.get(item))).count(),"enabledContinuousItems",enabledAreaProfiles.keySet().stream().filter(item->grenadeUseRules.get(item).handUse()&&GrenadeAreaScenes.continuous(enabledAreaProfiles.get(item))).count(),"scope","continuous-healing-shields-and-durable-pulses","renderedVerified",false));m.put("grenadeAreaStore",grenadeAreas.metrics());m.put("grenadeShields",Map.of("durability",grenadeAreas.shieldMetrics(),"blocked",shieldBlocked,"errors",shieldErrors,"pending",shieldShots.size(),"items",enabledAreaProfiles.keySet().stream().filter(item->grenadeUseRules.get(item).handUse()&&GrenadeAreaScenes.shield(enabledAreaProfiles.get(item))).count()));m.put("sickness",Map.of("profiles",sicknessRules.profiles().size(),"ticks",sicknessTicks,"hits",sicknessHits,"updates",sicknessUpdates,"players",sessions.values().stream().filter(p->p.ready).map(p->Map.of("name",p.grant.name(),"states",p.sickness.json(),"exposure",p.sicknessExposure)).toList(),"nativeTickOracle",false));m.put("underbarrels",Map.of("shots",underbarrelShots,"denied",underbarrelDenied,"reloads",underbarrelReloads,"canceledReloads",underbarrelReloadCanceled,"pendingReloads",sessions.values().stream().filter(s->s.launcherReload!=null).count(),"players",sessions.values().stream().filter(s->s.ready&&s.weapon!=null&&s.weapon.launcher()!=null).map(s->Map.of("name",s.grant.name(),"factory",s.weapon.launcher().factory(),"ammo",s.weapon.launcher().ammo(),"rounds",s.weapon.launcher().rounds(),"remembered",s.weapon.launcher().remembered())).toList()));m.put("jamming",Map.of("started",unjammingStarted,"completed",unjammingCompleted,"canceled",unjammingCanceled,"denied",unjammingDenied,"pending",sessions.values().stream().filter(s->s.unjamming!=null||s.unjammingPending).count(),"players",sessions.values().stream().filter(s->s.ready&&s.weapon!=null).map(s->Map.of("name",s.grant.name(),"jammed",s.weapon.jamming().jammed(),"duration",s.weapon.jamming().duration(),"pending",s.unjamming!=null||s.unjammingPending)).toList()));m.put("primaryLaunchers",Map.of("shots",primaryLauncherShots,"denied",primaryLauncherDenied));m.put("grenadeHealing",Map.of("applied",areaHealApplied,"denied",areaHealDenied,"pending",areaHealCommits.size(),"items",enabledAreaProfiles.keySet().stream().filter(item->grenadeUseRules.get(item).handUse()&&GrenadeAreaScenes.healing(enabledAreaProfiles.get(item))).count(),"players",sessions.values().stream().filter(s->s.ready).map(s->Map.of("name",s.grant.name(),"applications",inventories.areaApplicationCount(character(s)))).toList()));m.put("grenadePulses",Map.of("applied",pulseApplied,"denied",pulseDenied,"pending",pulseCommits.size(),"items",enabledAreaProfiles.keySet().stream().filter(item->grenadeUseRules.get(item).handUse()&&GrenadeAreaScenes.pulse(enabledAreaProfiles.get(item))).count(),"players",sessions.values().stream().filter(s->s.ready).map(s->Map.of("name",s.grant.name(),"entries",s.pulses.active(System.currentTimeMillis()).stream().map(e->Map.of("id",e.id(),"source",e.source(),"item",e.item(),"until",e.until(),"stats",e.definition().stats())).toList())).toList()));m.put("grenadeRevives",Map.of("applied",revives,"denied",reviveDenied,"errors",reviveErrors,"checks",reviveChecks.size(),"pending",reviveCommits.size(),"items",enabledAreaProfiles.values().stream().filter(GrenadeAreaScenes::supportUltimate).count()));m.put("grenadeRecovery",Map.of("restored",grenadeRecovered));m.put("grenadeUse",Map.of("held",sessions.values().stream().filter(s->s.grenadeUse.held()!=null).count(),"handExplosions",grenadeHeldExplosions,"canceled",grenadeUseCanceled,"prepareDenied",grenadePrepareDenied,"cookingProfiles",grenadeUseRules.values().stream().filter(GrenadeUse.Rules::ignites).count()));m.put("combatVisibility",Map.of("blocked",blockedHits,"errors",visibilityErrors));m.put("botCombat",Map.of("hits",botHits,"deaths",botDeaths,"errors",botDamageErrors,"completionDrops",combatCompletionDrops.get(),"queued",combatCompleted.size()));m.put("worldEdit",Map.of("allowedCharacters",worldEditors.size(),"pending",sessions.values().stream().filter(s->s.editor.busy()).count()));m.put("applicationDrain",Map.of("pendingLanes",parseReady.size(),"messageBudget",64,"timeBudgetMs",2,"yields",applicationBudgetYields));m.put("running",running);m.put("backend",accounts.metrics());m.put("playerRules",Map.of("entitled",sessions.values().stream().filter(s->s.ready&&playerRules.entitled(character(s))).count(),"administratorMode",sessions.values().stream().filter(s->s.ready&&administrator(s)).count()));m.put("characterPreferences",sessions.values().stream().filter(s->s.ready).map(s->Map.of("name",s.grant.name(),"character",character(s),"faction",s.faction,"mode",s.gameplayMode.name(),"entitled",playerRules.entitled(character(s)))).toList());m.put("sceneConfiguration",Map.of("access",sceneAccess.metrics(),"denied",sceneDenied,"failed",sceneFailed,"pendingReads",sessions.values().stream().mapToInt(s->s.sceneReads.size()).sum(),"pendingWrites",sessions.values().stream().mapToInt(s->s.sceneSaves.size()).sum()));m.put("locations",locations.metrics());m.put("locationResidency",locations.residencyMetrics());m.put("stateSaves",locations.stateWriteMetrics());m.put("playerState",sessions.values().stream().filter(s->s.ready).map(s->Map.of("name",s.grant.name(),"yaw",s.yaw,"pitch",s.pitch,"health",s.hp,"hand",s.handContainer,"holstered",s.holstered,"damagedAt",s.damageWall)).toList());m.put("warps",warps.metrics());m.put("environment",environment.metrics());m.put("weatherConfigurations",environmentAccess.configurations.metrics());m.put("portals",Map.of("sceneStale",sessions.values().stream().mapToLong(s->s.portalLookup.sceneStale()).sum(),"sceneEpoch",world.portalEpoch(),"queries",portalQueries,"activations",portalActivations,"errors",portalErrors,"pending",sessions.values().stream().filter(s->s.portalLookup.busy()).count(),"coldRetries",sessions.values().stream().mapToLong(s->s.portalLookup.retries()).sum(),"stale",sessions.values().stream().mapToLong(s->s.portalLookup.stale()).sum(),"timeouts",sessions.values().stream().mapToLong(s->s.portalLookup.timeouts()).sum()));m.put("teleports",Map.of("completed",teleports,"errors",teleportErrors,"staleInputs",staleTeleportInputs,"pending",sessions.values().stream().filter(s->s.teleport!=null).count()));m.put("nativeBlocks",Map.of("access",nativeBlocks.metrics(),"denied",nativeBlockDenied,"failed",nativeBlockFailed,"intents",nativeBlockIntents,"repairs",nativeBlockRepairs,"pendingWrites",sessions.values().stream().mapToInt(s->s.blockSaves.size()).sum(),"pendingRepairs",sessions.values().stream().mapToInt(s->s.blockRepairs.size()).sum()));m.put("terrainProjection",terrainProjection.metrics());m.put("chunkManifests",Map.of("service",chunkManifests.metrics(),"sent",manifestsSent,"entries",manifestEntries,"stale",manifestStale,"failures",manifestFailures,"outstanding",sessions.values().stream().mapToInt(s->s.window.outstanding()).sum(),"known",sessions.values().stream().mapToLong(s->s.window.known()).sum(),"radius",chunkSettings.distance(),"batchSize",chunkSettings.batchSize(),"sessions",sessions.values().stream().filter(s->s.ready).map(s->Map.of("name",s.grant.name(),"radius",s.window.radius(),"loaded",s.chunks.size(),"coverageRows",s.window.coverageRows(),"outstandingLimit",s.window.limit())).toList()));m.put("chunkDelivery",Map.of("cacheHits",chunkCacheHits,"fullResponses",chunkFullResponses,"duplicateDemands",chunkDuplicateDemands,"deferredDemands",chunkDeferredDemands,"released",chunkReleased,"receipts",sessions.values().stream().mapToInt(s->s.deliveries.size()).sum(),"pending",sessions.values().stream().mapToInt(s->s.pendingChunks.size()).sum(),"deferred",sessions.values().stream().mapToInt(s->s.window.deferred()).sum()));m.put("chunkFaults",Map.of("coordinates",sessions.values().stream().mapToLong(s->s.faults.size()).sum(),"rows",sessions.values().stream().mapToInt(s->s.faults.rows()).sum(),"queued",sessions.values().stream().mapToInt(s->s.window.faultPending()).sum()));m.put("tileRefreshes",tileRefreshes);m.put("bulkBlockRefreshes",bulkBlockRefreshes);m.put("inventory",inventories.metrics());m.put("vitalsCommits",Map.of("pendingMedicine",medicineCommits.size(),"pendingDeath",deathCommits.size(),"pendingAreaHeal",areaHealCommits.size(),"completed",vitalsCommits,"failures",vitalsCommitFailures));m.put("characterDeletion",characterDeletions.metrics());m.put("nativeAdmin",Map.of("accepted",nativeAdminAccepted,"denied",nativeAdminDenied,"validatedEchoes",nativeAdminEchoes,"queued",sessions.values().stream().mapToInt(s->s.nativeGrants.size()).sum(),"queuedBytes",sessions.values().stream().mapToInt(s->s.nativeGrantBytes).sum(),"pending",sessions.values().stream().filter(s->s.nativeGrantPending).count()));m.put("tick",tick);m.put("maxTickMs",maxTickNanos/1e6);m.put("tickTiming",Map.of("maxGapMs",maxTickGapNanos/1e6,"maxLateMs",maxTickLateNanos/1e6,"maxSocketWaitMs",maxSocketWaitNanos/1e6,"maxReceiveMs",maxReceiveNanos/1e6,"maxCompletionMs",maxCompletionNanos/1e6,"lateTicks",List.copyOf(lateTicks),"maxApplicationMs",maxApplicationNanos/1e6,"slowPackets",List.copyOf(slowPackets)));m.put("handshakeTiming",Map.of("maxCookieMs",maxHelloCookieNanos/1e6,"maxSessionMs",maxHelloSessionNanos/1e6,"maxChallengeMs",maxHelloChallengeNanos/1e6));m.put("skippedSlots",skipped);m.put("inputs",inputs);m.put("shots",shots);m.put("hits",hits);m.put("deaths",deaths);m.put("corpses",corpses.metrics());m.put("entityIds",entityIds.metrics());m.put("respawnTiming",Map.of("cooldownRejected",respawnCooldowns,"retries",respawnRetries,"delaySeconds",5));m.put("respawns",respawns);m.put("lastShot",lastShot);m.put("lastReload",lastReload);m.put("lastMelee",lastMelee);m.put("applicationCounts",Map.copyOf(applicationCounts));m.put("rejected",rejected);m.put("retries",retries);m.put("retired",retired);m.put("unknownPackets",Map.copyOf(unknown));m.put("datagrams",Map.copyOf(datagrams));m.put("connectionMismatch",connectionMismatch);m.put("transportSessions",sessions.values().stream().map(s->Map.of("accepted",s.accepted,"ready",s.ready,"rx0",s.lanes[0].rx,"rx1",s.lanes[1].rx,"held",s.lanes[0].received.size())).toList());m.put("stamina",sessions.values().stream().filter(s->s.ready).map(s->Map.of("name",s.grant.name(),"value",s.stamina,"maximum",s.maxStamina,"reported",s.observedStamina,"mode",s.mode)).toList());m.put("equipmentStats",sessions.values().stream().filter(s->s.ready).map(s->Map.of("name",s.grant.name(),"maximumHealth",s.maxHp,"stats",s.stats,"effects",s.effects.active(System.currentTimeMillis()))).toList());m.put("players",sessions.values().stream().filter(s->s.ready).map(s->Map.of("name",s.grant.name(),"entity",s.entity,"position",s.pos.clone(),"health",s.hp,"rounds",s.rounds,"reserve",s.reserve,"capacity",s.weapon==null?0:s.weapon.capacity(),"ammo",s.weapon==null?0:s.weapon.ammo(),"unacked",s.lanes[0].sent.size()+s.lanes[1].sent.size(),"queued",s.lanes[0].queued.size()+s.lanes[1].queued.size())).toList());m.put("playerResources",sessions.values().stream().filter(s->s.ready).map(s->Map.of("entity",s.entity,"stamina",s.stamina,"maxStamina",s.maxStamina,"maxHealth",s.maxHp,"effects",s.effects.active(System.currentTimeMillis()).stream().map(CharacterEffects.Effect::id).toList())).toList());m.put("playerPostures",sessions.values().stream().filter(s->s.ready).map(s->Map.of("entity",s.entity,"locomotion",s.posture.locomotion(),"previous",s.posture.previous(),"bodyHeight",CharacterScale.height(s.posture.locomotion(),s.characterScale),"velocity",s.velocity.clone(),"speed",s.speed,"ground",s.ground)).toList());m.put("characterScale",Map.of("lastError",lastScaleError,"players",sessions.values().stream().filter(p->p.ready).map(p->Map.of("entity",p.entity,"name",p.grant.name(),"scale",p.characterScale,"capable",p.scaleCapable,"ground",p.ground,"speed",p.speed,"pending",p.scaleChange!=null||p.scaleSaving)).toList()));m.put("combatBarriers",combatBarriers.metrics());m.put("hordeCombat",hordeCombat.metrics());m.put("zombies",zombies.status());metrics=Collections.unmodifiableMap(m);
    }
    private void bots(Session s){
        World.Snapshot snapshot=world.snapshot();List<World.View> nearby=snapshot.entities().stream().filter(view->view.health()>0&&Math.hypot(view.x()-s.pos[0],view.z()-s.pos[2])<=96).sorted(Comparator.comparingDouble(view->Math.hypot(view.x()-s.pos[0],view.z()-s.pos[2]))).limit(64).toList();
        botHeadings.keySet().retainAll(botEntities.keySet());
        List<PlayerReplication.State> states=new ArrayList<>();Set<BotKey> visible=new HashSet<>();for(World.View bot:nearby){BotKey key=botKey(bot);livingBotPoses.put(key,bot);visible.add(key);int entity=botEntities.computeIfAbsent(key,id->entityIds.actor());double[] position={bot.x(),bot.y(),bot.z()};float yaw=BotAnimation.heading(bot,botHeadings.getOrDefault(key,0f));botHeadings.put(key,yaw);
            if(s.bots.add(key)){boolean zombie=ZombieHorde.owns(bot.id());enqueue(s,0,szserver.client.NpcVisuals.spawn(entity,zombie?"Зомби":bot.kind()+"-"+bot.id().substring(0,Math.min(8,bot.id().length())),position,yaw,0,bot,tick));}
            var appearance=bot.presentation();if(!Objects.equals(s.botAppearances.get(key),appearance)){
                if(!appearance.attributes().isEmpty())enqueue(s,0,szserver.client.NpcVisuals.attributes(tick,entity,appearance.wire(),bot));
                s.botAppearances.put(key,appearance);
            }
            Float previous=s.botHealth.put(key,bot.health()),maximum=s.botMaximum.put(key,bot.maximumHealth());if(previous==null||previous!=bot.health()||maximum==null||maximum!=bot.maximumHealth())enqueue(s,0,szserver.client.NpcVisuals.vitals(tick,entity,bot.health(),bot.maximumHealth(),bot.aiming(),bot));
            states.add(new PlayerReplication.State(entity,position,yaw,0,PlayerAnimationPackets.component(BotAnimation.state(bot,yaw).swing(hordeCombat.swing(bot)))));
        }
        for(BotKey id:new ArrayList<>(s.bots))if(!visible.contains(id)){s.bots.remove(id);s.botHealth.remove(id);s.botMaximum.remove(id);s.botAppearances.remove(id);Integer entity=botEntities.get(id);if(entity!=null){s.botReplication.forget(entity);enqueue(s,0,Wire.frame(2028,Wire.bytes(o->o.writeInt(entity))));}}
        Lane output=s.lanes[0];
        if(output.queued.size()>32||output.sent.size()>=WINDOW){replicationDeferred++;return;}
        var delivery=s.botReplication.prepare(tick,s.entity,states);
        if(delivery!=null){int fragments=(delivery.frame().length+FRAGMENT-1)/FRAGMENT;
            if(output.queued.size()+fragments<=MAX_QUEUE){enqueue(s,0,delivery.frame());delivery.accepted().run();replicationFrames++;replicationRecords+=delivery.records();replicationBytes+=delivery.frame().length;}
            else replicationDeferred++;
        }
    }
    private CharacterLocations.PlayerState playerState(Session s){return new CharacterLocations.PlayerState(s.yaw,s.pitch,s.hp,s.handContainer,s.holstered,s.damageWall,s.stamina,s.sickness);}
    private void observePlayer(Session s){var state=playerState(s);if(s.teleport!=null&&locations.position(character(s))!=null)locations.observeState(character(s),state);else locations.observe(character(s),point(s.pos),state);}
    private void remove(Session s){environmentAccess.disconnect(s.environmentClient);if(!sessions.remove(s.address,s))return;screenshots.disconnected(s.incarnation);if(s.grant!=null)characterSessions.retire(character(s),s);for(AreaScene area:areaScenes.values())area.recipients.remove(new Recipient(s.address,s.incarnation));s.grenadeUse.cancel();parseReady.removeIf(target->target.session()==s);for(Lane lane:s.lanes)lane.parsing=false;clones.forget(s);s.poses.clear();detachVehicleContact(s);vehicleContacts.forget(s);clearVehicleAction(s);for(var car:vehicles.forget(s))emitVehicle(car,VehicleWire.DESPAWN);recallFpv(s);for(Set<Session> observers:droneObservers.values())observers.remove(s);s.fpvWorldSent.clear();entityIds.release(s.entity);if(s.ready&&!s.respawning&&locations.healthy())try{observePlayer(s);savePlayerState(s);}catch(IllegalArgumentException ignored){}locations.flush();if(s.locationLease!=null){s.locationLease.close();s.locationLease=null;}if(s.inventoryPinned){inventories.deactivate(character(s));s.inventoryPinned=false;}retired++;for(Session other:new ArrayList<>(sessions.values()))if(other.ready&&s.ready){other.poses.remove(s.entity);other.replication.forget(s.entity);if(other.scaleCapable)enqueueOrRetire(other,CharacterScale.remove(s.entity,++scaleRevision));enqueueOrRetire(other,Wire.frame(2028,Wire.bytes(o->o.writeInt(s.entity))));}}
    @Override public void close()throws Exception{
        NpcEditorHooks.close(this);
        synchronized(this){if(closed)return;closed=true;}commands.unbindPlayerInventories(adminInventories);
        Exception failure=null;try{adminInventories.close();}catch(Exception error){failure=error;}
        running=false;for(AutoCloseable socket:List.of(gateway,udp))try{socket.close();}catch(Exception error){if(failure==null)failure=error;else failure.addSuppressed(error);}
        try{transport.join(2000);acceptor.join(2000);}catch(InterruptedException error){Thread.currentThread().interrupt();if(failure==null)failure=error;else failure.addSuppressed(error);}
        if(transport.isAlive()||acceptor.isAlive()){IllegalStateException timeout=new IllegalStateException("game_shutdown_timeout");if(failure!=null)timeout.addSuppressed(failure);throw timeout;}
        screenshots.close();
        try{Workers.stop(logins);}catch(Exception error){if(failure==null)failure=error;else failure.addSuppressed(error);}
        try{
            long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(10);
            while(grenadeBridge.pending()!=0){inventories.drain();grenadeBridge.drain();if(System.nanoTime()>deadline)throw new IllegalStateException("grenade_shutdown_admission_timeout");Thread.sleep(2);}
            if(grenadeSave!=null)grenadeSave.get(10,TimeUnit.SECONDS);
            var finalState=world.submit(w->w.grenades().freeze()).get(10,TimeUnit.SECONDS);
            grenadeOutbox.save(finalState,List.of()).get(10,TimeUnit.SECONDS);
        }catch(Exception error){if(failure==null)failure=error;else failure.addSuppressed(error);}
        try{long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(10);while(!shieldShots.isEmpty()||combatBarriers.pending()!=0){advanceShieldShots();combatBarriers.advance();if(System.nanoTime()>deadline)throw new IllegalStateException("shield_shutdown_timeout");Thread.sleep(2);}}catch(Exception error){if(failure==null)failure=error;else failure.addSuppressed(error);}
        for(var check:reviveChecks.values())check.placement().cancel(false);reviveChecks.clear();
        // Accepted inventory callbacks may still change vitals. Keep locations/codecs open until they finish.
        for(AutoCloseable resource:List.of(characterDeletions,accounts,inventories))try{resource.close();}catch(Exception error){if(failure==null)failure=error;else failure.addSuppressed(error);}
        try{long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(10);while(!medicineCommits.isEmpty()||!deathCommits.isEmpty()||!areaHealCommits.isEmpty()||!pulseCommits.isEmpty()||!reviveCommits.isEmpty()){advanceVitals();if(System.nanoTime()>deadline)throw new IllegalStateException("vitals_shutdown_timeout");Thread.sleep(2);}}catch(Exception error){if(failure==null)failure=error;else failure.addSuppressed(error);}
        for(Session s:sessions.values())if(s.ready&&!s.respawning&&locations.healthy())try{observePlayer(s);savePlayerState(s);}catch(Exception error){if(failure==null)failure=error;else failure.addSuppressed(error);}
        for(AutoCloseable resource:List.of(locations,warps,sceneAccess,environmentAccess,nativeBlocks,chunkManifests,terrainProjection,nativePackets,worldItems,fpvMap,fpvReceipts,grenadeOutbox,grenadeAreas,grenadeAreaScenes))try{resource.close();}catch(Exception error){if(failure==null)failure=error;else failure.addSuppressed(error);}
        if(failure!=null)throw failure;
    }
}
