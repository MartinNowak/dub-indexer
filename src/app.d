import vibe.db.mongo.mongo;
import vibe.core.core : exitEventLoop, runTask, runEventLoop;
import vibe.data.bson;
import std.datetime.systime, std.stdio;
import std.algorithm : find, map, sort;
import std.conv : to;
import std.path;
import std.file : chdir, exists, mkdirRecurse;
import git;

struct DbPackage
{
    string name;
    DbPackageVersion[] versions;
}

template StripPolicy(T)
    if (is(T == DbPackageVersion) || is(T == DbPackageVersion.Info))
{
    // Overloading the StripPolicy template doesn't work with
    // isPolicySerializable, so use a single template and forward to an
    // overloaded module-level function instead.
    //
    // CustomSerializable cannot be used as those methods would interfere with
    // the Bson deserialization.
    Json toRepresentation(T val) @safe { return .toRepresentation(val); }
    T fromRepresentation(Json) @safe { assert(0); }
}

Json toRepresentation(DbPackageVersion.Info info) @safe
{
    import std.array : array;

    auto doc = Json.emptyObject;
    if (info.name) // sub-packages
        doc["name"] = info.name;
    if (info.dependencies.length)
        doc["dependencies"] = info.dependencies;
    if (info.targetType != info.targetType.autodetect)
        doc["targetType"] = info.targetType.to!string;
    if (info.configurations.length)
    {
        auto cfgs = Json.emptyArray;
        foreach (cfg; info.configurations)
        {
            auto cfgdoc = Json.emptyObject;
            cfgdoc["name"] = cfg.name;
            if (cfg.targetType != cfg.targetType.autodetect)
                cfgdoc["targetType"] = cfg.targetType.to!string;
            if (cfg.dependencies.length)
                cfgdoc["dependencies"] = cfg.dependencies;
            cfgs ~= cfgdoc;
        }
        doc["configurations"] = cfgs;
    }
    if (info.subPackages.length)
    {
        doc["subPackages"] = info.subPackages
            .map!(p => p.serializeWithPolicy!(JsonSerializer, StripPolicy)).array;
    }
    return doc;
}

Json toRepresentation(DbPackageVersion ver) @safe
{
    auto doc = ver.info.serializeWithPolicy!(JsonSerializer, StripPolicy);
    doc["version"] = ver.version_;
    return doc;
}

struct DbPackageVersion
{
    SysTime date;
    string version_;
    @optional string commitID;

    static struct Info
    {
        @optional string name; // optional when used for package version
        enum TargetType { autodetect, none, executable, library, sourceLibrary, staticLibrary, dynamicLibrary, }
        @optional @byName TargetType targetType;
        static struct Configuration {
            enum TargetType { autodetect, none, executable, library, sourceLibrary, staticLibrary, dynamicLibrary, }
            @optional @byName TargetType targetType;
            string name;
            @optional Json[string] dependencies;
        }
        @optional Configuration[] configurations;
        @optional Json[string] dependencies;
        @optional Info[] subPackages;
    }
    @optional Info info;
}

static assert(isPolicySerializable!(StripPolicy, DbPackageVersion));
static assert(isPolicySerializable!(StripPolicy, DbPackageVersion.Info));

string repoPath(string name)
{
    assert(name.length);
    switch (name.length)
    {
    case 0: assert(0);
    case 1: return "1/"~name;
    case 2: return "2/"~name;
    case 3: return "3/"~name[0]~"/"~name;
    default: return name[0..2]~"/"~name[2..4]~"/"~name;
    }
}

unittest
{
    assert(repoPath("a" == "1/a"));
    assert(repoPath("ab" == "2/ab"));
    assert(repoPath("abc" == "3/a/abc"));
    assert(repoPath("abcd" == "ab/cd/abcd"));
    assert(repoPath("abcde" == "ab/cd/abcde"));
    assert(repoPath("abcdefghijklmn" == "ab/cd/abcdefghijklmn"));
}

void asyncMain(GitRepo repo)
{
    import std.array : appender, empty, front;
    import std.typecons : tuple, Tuple;
    auto packages = connectMongoDB("mongodb://localhost").getDatabase("vpmreg")["packages"];
    packages.update(Bson.emptyObject, ["$pull": ["versions.$[].info.subPackages": ["name": ["$exists": false]]]], UpdateFlags.multiUpdate);
    packages.update(Bson.emptyObject, ["$pull": ["versions.$[].info.subPackages": ["$type": "string"]]], UpdateFlags.multiUpdate);
    packages.update(["versions.info.configurations.default": ["$exists": true]], ["$unset": ["versions.$[].info.configurations": ""]], UpdateFlags.multiUpdate);

    Tuple!(string, DbPackageVersion)[] pkgVers;
    try
    {
        auto a = appender!(typeof(pkgVers));
        foreach (pkg; packages.find.map!(deserializeBson!DbPackage))
            foreach (ver; pkg.versions)
                a.put(tuple(pkg.name, ver));
        pkgVers = a.data;
    }
    catch (Exception e)
        return writeln(e);
    pkgVers.sort!((a, b) => a[1].date < b[1].date);
    size_t cnt;
    GitCommit head;
    if (!repo.isHeadOrphan)
    {
        head = repo.lookupCommit(repo.head.target);
        pkgVers = pkgVers.find!(v => v[1].date > head.commitTime);
    }
    foreach (name, ver; pkgVers.map!(e => e))
    {
        if (cnt++ % 512 == 0)
        {
            writef!" %2.0f %% (%5s/%5s commits)\r"(100.0 * cnt / pkgVers.length, cnt, pkgVers.length);
            stdout.flush;
        }
        immutable path = repoPath(name);
        mkdirRecurse(dirName(path));
        File(path, "a").writeln(ver.serializeWithPolicy!(JsonSerializer, StripPolicy));
        auto idx = repo.index;
        idx.addByPath(path);
        idx.write;
        auto sig = createSignature("The Dlang Bot", "code+dlang-bot@dawg.eu", ver.date);
        if (repo.isHeadOrphan) // HEAD unborn, create initial commit
            head = repo.lookupCommit(repo.createCommit("HEAD", sig, sig, name, repo.lookupTree(idx.writeTree)));
        else
            head = repo.lookupCommit(repo.createCommit("HEAD", sig, sig, name, repo.lookupTree(idx.writeTree), head));
    }
    stdout.writeln;
    exitEventLoop;
}

void main()
{
    auto repo = "repo".exists ? "repo".openRepository : "repo".initRepository(OpenBare.no);
    chdir("repo");
    runTask(&asyncMain, repo);
    runEventLoop;
}
