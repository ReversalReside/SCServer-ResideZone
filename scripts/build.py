"""Source-only developer build. Never reads installed game files or deploys a server."""
from pathlib import Path
import argparse,hashlib,json,os,shutil,subprocess,tempfile,time,urllib.error,urllib.request,zipfile

ROOT=Path(__file__).resolve().parents[1]
B=ROOT/'build'

def sha(path):
    h=hashlib.sha256()
    with path.open('rb') as source:
        for chunk in iter(lambda:source.read(1048576),b''):h.update(chunk)
    return h.hexdigest()

def tool(name):
    home=os.environ.get('JAVA_HOME')
    path=Path(home)/'bin'/(name+('.exe' if os.name=='nt' else '')) if home else shutil.which(name)
    if not path or not Path(path).is_file():raise SystemExit('Install JDK 17 and configure JAVA_HOME/PATH; missing '+name)
    return str(path)

def dependency_bytes(row):
    urls=[row['url']]
    prefix='https://repo.maven.apache.org/maven2/'
    if row['url'].startswith(prefix):
        suffix=row['url'][len(prefix):]
        urls+=['https://repo1.maven.org/maven2/'+suffix,
               'https://maven-central.storage-download.googleapis.com/maven2/'+suffix]
    last=None
    for attempt in range(3):
        for url in urls:
            try:
                request=urllib.request.Request(url,headers={'User-Agent':'SCServer-source-build/1'})
                with urllib.request.urlopen(request,timeout=30) as source:raw=source.read(32*1024*1024+1)
            except (urllib.error.URLError,TimeoutError) as error:
                last=error;continue
            if len(raw)>32*1024*1024 or hashlib.sha256(raw).hexdigest()!=row['sha256']:
                raise SystemExit('Dependency checksum mismatch: '+row['file'])
            return raw
        if attempt<2:time.sleep(1+attempt*2)
    raise SystemExit('Cannot download '+row['file']+': '+str(last))


def dependencies():
    directory=ROOT/'.local/deps';directory.mkdir(parents=True,exist_ok=True);result=[]
    for row in json.loads((ROOT/'dependencies.lock.json').read_text('utf8')).values():
        path=directory/row['file']
        if not path.exists():
            print('Fetching '+row['file'],flush=True)
            path.write_bytes(dependency_bytes(row))
        if sha(path)!=row['sha256']:raise SystemExit('Cached dependency differs: '+str(path))
        result.append(path)
    return result

def compile_java(name,sources,classpath=()):
    output=B/(name+'-classes')
    if output.exists():
        if not output.resolve().is_relative_to(ROOT/'build'):raise SystemExit('Refuse build output outside checkout')
        shutil.rmtree(output)
    output.mkdir(parents=True)
    args=['--release','17','-encoding','UTF-8','-proc:none','-d',str(output)]
    if classpath:args+=['-cp',os.pathsep.join(map(str,classpath))]
    args+=list(map(str,sorted(sources)))
    argfile=B/(name+'-javac.args')
    argfile.write_text('\n'.join('"'+arg.replace('\\','/').replace('"','\\"')+'"' for arg in args),'utf8')
    subprocess.run([tool('javac'),'-J-Dfile.encoding=UTF-8','@'+str(argfile)],check=True,cwd=ROOT)
    return output

def jar(name,classes,extra=()):
    target=B/(name+'.jar')
    with zipfile.ZipFile(target,'w',zipfile.ZIP_DEFLATED) as out:
        for p in sorted(classes.rglob('*.class')):
            entry=zipfile.ZipInfo(p.relative_to(classes).as_posix(),(2020,1,1,0,0,0));entry.compress_type=zipfile.ZIP_DEFLATED;out.writestr(entry,p.read_bytes())
        for path,dest in extra:
            entry=zipfile.ZipInfo(dest,(2020,1,1,0,0,0));entry.compress_type=zipfile.ZIP_DEFLATED;out.writestr(entry,path.read_bytes())
    return target

def server(private_resources=False):
    deps=dependencies();classes=compile_java('server',[*(ROOT/'server/src/main/java').rglob('*.java'),*(ROOT/'shared/player-scale/src').rglob('*.java')],deps)
    extra=[]
    if private_resources:
        for name,digest in json.loads((ROOT/'config/private-resources.lock.json').read_text('utf8')).items():
            path=ROOT/'.local/server-resources'/name
            if not path.is_file() or sha(path)!=digest:raise SystemExit('Missing or changed private resource: '+name+'; run scripts/import_private_resources.py')
            extra.append((path,name))
    jar('server',classes,extra);return classes,deps

def sdk():
    classes=compile_java('sdk',(ROOT/'sdk/api').rglob('*.java'));api=jar('mod-api',classes)
    example=compile_java('hello',(ROOT/'sdk/example/src').rglob('*.java'),[api])
    jar('hello-mod',example,[(ROOT/'sdk/example/stalcraft-mod.properties','stalcraft-mod.properties')])
    return api

def completion():
    api=sdk();deps=dependencies();source=ROOT/'client/command-completion'
    classes=compile_java('command-completion',(source/'src').rglob('*.java'),[api]+deps)
    jar('command-completion',classes,[(source/'stalcraft-mod.properties','stalcraft-mod.properties')])
    test=compile_java('command-completion-tests',(source/'tests').rglob('*.java'),[classes])
    subprocess.run([tool('java'),'-cp',os.pathsep.join(map(str,[test,classes])),'completion.CompletionTest'],check=True)

def tests():
    classes,deps=server();completion()
    test_classes=compile_java('tests',(ROOT/'server/src/test/java').rglob('*.java'),[classes]+deps)
    cp=os.pathsep.join(map(str,[test_classes,classes]+deps));results=[]
    subprocess.run([tool('java'),'-Xmx128m','-cp',cp,'szserver.EquipmentPlacementTests'],check=True)
    with tempfile.TemporaryDirectory(prefix='inventory-sort-',dir=B/'scratch' if (B/'scratch').exists() else B) as directory:
        subprocess.run([tool('java'),'-Xmx128m','-cp',cp,'szserver.inventory.InventorySortTests',directory],check=True)
    results.append('inventory sort merges identical stacks, packs by family, covers bag/storage and packet 1506 layout')
    subprocess.run([tool('java'),'-Xmx128m','-cp',cp,'szserver.world.RegionRecordBoundsTests'],check=True)
    subprocess.run([tool('java'),'-Xmx128m','-cp',cp,'szserver.LootboxTests'],check=True)
    subprocess.run([tool('java'),'-Xmx128m','-cp',cp,'szserver.WorldItemTests'],check=True)
    subprocess.run([tool('java'),'-Xmx128m','-cp',cp,'szserver.world.WorldItemPhysicsTests'],check=True)
    subprocess.run([tool('java'),'-Xmx128m','-cp',cp,'szserver.client.NativeMethodsTests'],check=True)
    subprocess.run([tool('java'),'-Xmx128m','-cp',cp,'szserver.AnomalyRadiusTests'],check=True)
    subprocess.run([tool('java'),'-Xmx128m','-cp',cp,'szserver.AnomalyTests'],check=True)
    subprocess.run([tool('java'),'-Xmx128m','-cp',cp,'szserver.world.AnomalyIndexTests'],check=True)
    results.append('native return descriptor, bridge, parameters and static exclusion: 5 PASS')
    results.append('region slot/overflow/truncation/CRC/healthy-neighbour bounds')
    subprocess.run([tool('java'),'-Xmx128m','-cp',cp,'szserver.inventory.StatValuesTests'],check=True)
    results.append('native unlimited protection remains finite; unrelated invalid stats rejected')
    subprocess.run([tool('java'),'-Xmx128m','-cp',cp,'szserver.inventory.WeaponResourceTests'],check=True)
    results.append('authored admin Gauss accepted; nonfinite weapon parameters rejected')
    # These are wholly owned synthetic files, not game data or user saves.
    scratch=B/'scratch';scratch.mkdir(exist_ok=True)
    with tempfile.TemporaryDirectory(prefix='native-grid-',dir=scratch) as directory:
        subprocess.run([tool('java'),'-Xmx128m','-cp',cp,'szserver.inventory.NativeGridMigrationTests',directory],check=True)
    with tempfile.TemporaryDirectory(prefix='character-scale-',dir=scratch) as directory:
        subprocess.run([tool('java'),'-Xmx128m','-cp',cp,'szserver.net.CharacterScaleTests',directory],check=True)
    results.append('character scale native dimensions, bounded commands, optional-save recovery, native ready bounds and ceiling admission')
    results.append('native grid migration preserves complete items; full bag overflow and interrupted-marker retry')
    with tempfile.TemporaryDirectory(prefix='journal-',dir=scratch) as directory:
        root=Path(directory);(root/'var').mkdir();(root/'results').mkdir()
        subprocess.run([tool('java'),'-Xmx64m','-cp',cp,'szserver.JournalStreamingTests',str(root)],check=True)
        report=json.loads((root/'results/test-journal-streaming.json').read_text('utf8'))
        results.append({'journalAssertions':report['assertions'],'records':report['records']})
    with tempfile.TemporaryDirectory(prefix='gateway-',dir=scratch) as directory:
        subprocess.run([tool('java'),'-Xmx128m','-cp',cp,'szserver.net.GatewayRetryTests',directory],check=True)
    results.append('gateway same/fresh-token retry, invalid identity/session, single-use world claim')
    with tempfile.TemporaryDirectory(prefix='authentication-stability-',dir=scratch) as directory:
        subprocess.run([tool('java'),'-Xmx128m','-cp',cp,'szserver.net.AuthenticationStabilityTests',directory],check=True)
    results.append('72 full HTTP/gateway/world cycles, Unicode identities, concurrent protocols and cold one-shot recovery')
    (B/'verification.json').write_text(json.dumps({'result':'PASS','checks':results,'gameFilesUsed':False,'networkSessionTested':False},indent=2),'utf8')

def desktop():
    if os.name!='nt':raise SystemExit('Desktop build requires Windows and .NET Framework C# compiler')
    compiler=Path(os.environ.get('WINDIR','C:/Windows'))/'Microsoft.NET/Framework64/v4.0.30319/csc.exe'
    if not compiler.exists():raise SystemExit('Missing .NET Framework C# compiler')
    destination=B/'desktop';destination.mkdir(parents=True,exist_ok=True)
    for symbol,name in [('CLIENT','Launcher'),('SERVER','ServerManager')]:
        subprocess.run([str(compiler),'/nologo','/target:winexe','/platform:x64','/optimize+','/define:'+symbol,'/out:'+str(destination/(name+'.exe')),'/reference:System.dll,System.Core.dll,System.Windows.Forms.dll,System.Drawing.dll,System.Web.Extensions.dll',str(ROOT/'desktop/src/GameLayout.cs'),str(ROOT/'desktop/src/Portable.cs')],check=True)
    subprocess.run([str(compiler),'/nologo','/target:winexe','/platform:x64','/optimize+','/define:SERVER','/main:LocalProgram','/out:'+str(destination/'Играть.exe'),'/reference:System.dll,System.Core.dll,System.Windows.Forms.dll,System.Drawing.dll,System.Web.Extensions.dll',str(ROOT/'desktop/src/GameLayout.cs'),str(ROOT/'desktop/src/Portable.cs'),str(ROOT/'desktop/src/LocalLauncher.cs')],check=True)
    subprocess.run([str(compiler),'/nologo','/target:winexe','/platform:x64','/optimize+','/main:SetupProgram','/out:'+str(destination/'BSCPCRAFT-Setup.exe'),'/reference:System.dll,System.Core.dll,System.Windows.Forms.dll,System.Drawing.dll,System.Web.Extensions.dll,System.IO.Compression.dll,System.IO.Compression.FileSystem.dll',
                   '/resource:'+str(destination/'Играть.exe')+',BootLocal','/resource:'+str(destination/'Launcher.exe')+',BootClient','/resource:'+str(destination/'ServerManager.exe')+',BootServer',str(ROOT/'desktop/src/GameLayout.cs'),str(ROOT/'desktop/src/Setup.cs')],check=True)
    test=destination/'SetupSafetyTests.exe'
    subprocess.run([str(compiler),'/nologo','/target:exe','/platform:x64','/define:SERVER','/main:SetupSafetyTests','/out:'+str(test),'/reference:System.dll,System.Core.dll,System.Windows.Forms.dll,System.Drawing.dll,System.Web.Extensions.dll,System.IO.Compression.dll,System.IO.Compression.FileSystem.dll',
                   '/resource:'+str(destination/'Играть.exe')+',BootLocal','/resource:'+str(destination/'Launcher.exe')+',BootClient','/resource:'+str(destination/'ServerManager.exe')+',BootServer',str(ROOT/'desktop/src/Setup.cs'),str(ROOT/'desktop/src/GameLayout.cs'),str(ROOT/'desktop/src/Portable.cs'),str(ROOT/'desktop/tests/SetupSafetyTests.cs')],check=True)
    scratch=B/'scratch';scratch.mkdir(exist_ok=True)
    with tempfile.TemporaryDirectory(prefix='setup-safety-',dir=scratch) as directory:
        subprocess.run([str(test),directory],check=True)
    client_test=destination/'ClientPreflightTests.exe'
    subprocess.run([str(compiler),'/nologo','/target:exe','/platform:x64','/define:CLIENT','/main:ClientPreflightTests','/out:'+str(client_test),'/reference:System.dll,System.Core.dll,System.Windows.Forms.dll,System.Drawing.dll,System.Web.Extensions.dll',str(ROOT/'desktop/src/GameLayout.cs'),str(ROOT/'desktop/src/Portable.cs'),str(ROOT/'desktop/tests/ClientPreflightTests.cs')],check=True)
    with tempfile.TemporaryDirectory(prefix='client-preflight-',dir=scratch) as directory:
        subprocess.run([str(client_test),directory,str(ROOT/'desktop/src/Portable.cs')],check=True)

if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('target',choices=['server','sdk','command-completion','test','desktop']);parser.add_argument('--private-resources',action='store_true',help='Package pinned local game resources; never upload this JAR');args=parser.parse_args()
    if args.private_resources and args.target!='server':parser.error('--private-resources requires target server')
    B.mkdir(exist_ok=True)
    if args.target=='server':server(args.private_resources)
    else:{'sdk':sdk,'command-completion':completion,'test':tests,'desktop':desktop}[args.target]()
