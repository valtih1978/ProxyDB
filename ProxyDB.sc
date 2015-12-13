import java.lang.reflect.{Method, InvocationHandler, Proxy}
import scala.language.{reflectiveCalls, existentials}, scala.language.postfixOps
import scala.collection.mutable, java.io._, Utils._

// This implementaion can run all the application (ProxyDemo, PersistenceDemo and MutabilityDemo)
// from first implementation. However, in contrast with the reduced version, it enables proxies
// to GC, which is supposed to be more efficient in the long run, in case we have really many
// managed objects. Experiment
	// timeit "scala -J-Xmx33m ProxyDemo + 16 100 > nul"
// suggests that this is faster indeed.

// Probably this does not make a lot of sense as long as we store each object in separate file
// because maintaining a large folder is more memory demanding than maintaining the proxy list.
// At least I must compare how sooner first version fails than this one.

//java_home=c:\Program Files\java\jre1.8.0_45
// timeit "scala -J-Xmx1000m ProxyDemo + 18 1k > nul" // 846 sec

class Db(dir: File, var cacheSize: Int, clean: Boolean) {
	
	import java.lang.ref._
	
	System.err.println(s"hard cache size $cacheSize objects, dir = $dir")
	
	// We probably do not need this since soft references are employed.
	val hardCache = mutable.Queue[Proxy]()
	
	// I would like to merge the ProxyClass with WeakReference but
	// constructor of weakref needs a proxy, which needs proxy class
	// for its constructor.
	// When proxee != null in constructor, must set drity = Yes and call activate. This will force serialization on eviction.
	class ProxyClass (
		val dbid: Int, var proxee: Object, //  not null proxees must be exacly in the actives
		val implements: Class[_],
		var dirty: Boolean) extends InvocationHandler { rqClean
	
	// Physical ID might be used to support mutability. When object
	// is updated, a new revision is created. In append-only DB,
	// physical ID may be the offset of the serialized object
		//var physicalDBID: Int
	
		//println("creating " + this)
		
		// When new object is created we may evict something from mem
		
	  override def invoke(proxy: Object, method: Method, args: Array[Object]): AnyRef = {

			//println("calling1 " + this +"."+ method.getName + ", victims = " + victims)
			
			assert(weakProxies(dbid).get == proxy, "proxy in the weak map mismatches the invoked one")
			//assert(wr.get eq proxy, this + s" is accessed through ${System.identityHashCode(proxy)} but is known as ${wr.get} in our weakMap")
			
			proxee = proxee match {
				case ref: Reference[Object] => ref.get //val res = ref.get ; if (res != null) println(s"restored $this from memory") ; res
				case a => a
			}
			
			if (proxee == null) { //println(s"restoring $this from disk")
				proxee = closing(new ObjectInputStream(new FileInputStream(fname(dbid)))){
					in => in.readObject match {
						case ts: ProxifiableField => ts.readFields(in, Db.this) ; ts
						case o => o // this is conventional object
					}
				}
			}
			
			activate(proxy.asInstanceOf[Proxy])
			
			//println("calling2 " + this +"."+ method.getName)
			
			invokeProxee(method, args)
			
	  }
	  
	  def invokeProxee(method: Method, args: Array[Object]): AnyRef = {
	    method.invoke(proxee, args: _*)
	   }
	  
		def serializeDirty = if (dirty) {
			//println(dbid + " was dirty")
			closing(new ObjectOutputStream(new FileOutputStream(fname(dbid))))
			{ oos => oos.writeObject(proxee) ; proxee match {
						case ts: ProxifiableField => ts.writeFields(oos, Db.this)
						case _ => // this is conventional object
			}} ; dirty = false
			
		}

	  override def toString = (if (proxee == null) "-" else "+") + dbid
	}

	
	def activate(proxy: Proxy) {
		hardCache.dequeueAll(_ eq proxy)
		evict(cacheSize)
		hardCache.enqueue(proxy)
	}
	
	def evict(threshold: Int) =
		while (hardCache.length > threshold) {
			val pc = proxyClass(hardCache.dequeue())
			val dbid = pc.dbid ; val proxee = pc.proxee
			//println("passivating " + dbid)
			assert(hardCache map proxyClass forall { _.proxee != null }) // check that all are active
			pc.serializeDirty // after serialization, dirty can be clean or updating
			pc.proxee = softCache(proxee)
		}
	
	
	// setting softCache = {_ => null} disables memory cache and forces use of the disk alone, useful for testing
	var softCache = (proxee: Object) => new SoftReference(proxee)
	
	def proxyClass(p: Proxy) = Proxy.getInvocationHandler(p).asInstanceOf[ProxyClass]
	
	def victims = hardCache.map(proxyClass(_)).mkString(",")


	// includes both active and not-yet-GC'ed proxies
	val weakProxies: mutable.Map[Int, MyWR] = mutable.Map()
	
	var total: Int = _

	//import scala.reflect.runtime.universe._
	//def getType[T: TypeTag](a: T): Type = typeOf[T]
	def put[T <: AnyRef with Serializable, I](proxee: T, supportedInterfaces: Class[I]) = {
		total += 1 ;
		val pc = new ProxyClass(total, proxee, supportedInterfaces, true)
		val proxy = makeProxy(pc).asInstanceOf[Proxy]
		activate(proxy) ; proxy.asInstanceOf[I] // need to activate coz deactivation serializes the object
 	}

	
	private val rq = new ReferenceQueue[Proxy]() ; var finalizedPC = 0
	@annotation.tailrec final def rqClean {rq.poll match {
		case null =>
		case top =>	finalizedPC += 1; val dbid = top.asInstanceOf[MyWR].dbid
			assert(weakProxies.contains(dbid)) ; val current = weakProxies(dbid)
			//println((if (current != top) "a previous copy of " else "") + s"proxy with dbid " + dbid + " was finalized")
			if (current == top) weakProxies -= dbid ; rqClean
	}}
	
	//class HardReference[T](val referent: Proxy, rq1: Object) extends SoftReference[Proxy](referent, rq)
	class MyWR(val dbid: Int, proxy: Proxy) extends WeakReference[Proxy](proxy, rq)
	
	//pc must have proxee initialized
	def makeProxy(pc: ProxyClass) = {
		assert(!weakProxies.contains(pc.dbid) || weakProxies(pc.dbid).get == null)
		val proxy = Proxy.newProxyInstance(pc.implements.getClassLoader, Array(pc.implements), pc)
   	weakProxies(pc.dbid) = new MyWR(pc.dbid, proxy.asInstanceOf[Proxy])
   	proxy
	}
	
 	def fname(dbid: Int/*, physicalID: Int*/): File = fname("" + dbid/* + "." + physicalID*/)
 	def fname(dbid: String) = new File(dir + "/" + dbid)

 	// names are used for persistence
 	val names = mutable.Map[String, (Int, Class[_])]() ; val fNames = fname("roots.lst")
 	def name(name: String, p: Object) = {
 		val pc = proxyClass(p.asInstanceOf[Proxy])
 		val interface = pc.implements // must be in the map coz proxy is referenced by thread
 		names(name) = (pc.dbid, pc.implements) ; pc.dbid
 	}
 	def byName[T](name: String) = {
 		val (dbid, interface) = names(name)
 		fromWeak(dbid, interface).asInstanceOf[T]
 	}

	object updating {
	
		val active = mutable.Map[ProxyClass, Int]().withDefaultValue(0)
	
		//proxy will stay in memory while apply is executing because executing thread references it
		def apply[T](proxy: Object)(code: => T) = {
			val pc = proxyClass(proxy.asInstanceOf[Proxy])
			active(pc) += 1 ; hardCache.dequeueAll(_ eq proxy)
			try code finally {
				rqClean ; val re = active(pc) -1
				if (re != 0) active(pc) = re else {
					pc.dirty = true ; active -= pc
					activate(proxy.asInstanceOf[Proxy]) // need to activate since it is eviction from hard cache sends our update object to disk saver
				}
			}
 		}
 	}

	def flush {
		//serialize(fNames, names toList)
		closing(new PrintWriter(fNames))( pw => names.foreach {
			case (n, (dbid, interface)) => pw.println(n + " " + dbid + " " + interface.getName)
		})
		
		hardCache map proxyClass foreach { _.serializeDirty}
	}
		
	def close() {
		rqClean ; System.err.println(s"finalized + " + decimal(finalizedPC) + ", number of proxies remaining known to the db = " + weakProxies.size)
		flush ; hardCache.clear ; names.clear
		// stop any threads if running
		// can fail in the assertion once all resources closed?
		//assert (updating.active.size == 0 , "there are incomplete updates: " + updating.active.keys.mkString(","))
		if(updating.active.size != 0) System.err.println("WARNING! there are incomplete updates: " + updating.active.keys.mkString(","))
 	}
	def open() = {
		closing(scala.io.Source.fromFile(fNames))(_.getLines().foreach {
			line => val nameval = line split ' ' take 3
							names(nameval(0)) = (nameval(1) toInt, class4name(nameval(2)))
		})
		
		total = dir.list().length - 1 // one file is for names, other are regular objects
	}
	def restart() = { close ; open }

	if (clean) {
	 	dir.mkdir()
	 	dir.listFiles().foreach { f => java.nio.file.Files.deleteIfExists(f.toPath())}
	 	assert(dir.list().length == 0)
	 	fNames.createNewFile()
	}
	
	open()

	def fromWeak(dbid: Int, implements: Class[_]) = {
		weakProxies.get(dbid) map {_.get} filter(_!= null) getOrElse {
			makeProxy(new ProxyClass(dbid, null, implements, false))
		}
	}
}


// Every class, whose fields can be proxified, must implement
// this trait.
	//todo: use scala marcos obiate the need to call wr(field),
	// field = rd(). User could just list the fields to the macros.
	// Even better could be if we could collect the transient fields
	// ourselves at compilation time.


trait ProxifiableField extends Serializable {
	
	// WrtieFields is called during serialization. User must
	// call wr(field) for every @transient-marked field
	def writeFields(out: ObjectOutputStream, db: Db)
	
	// ReadFields must call field = rd(in, db) for every transient field
	def readFields(in: ObjectInputStream, db: Db)

	// auxillary methods
	def wr(out: ObjectOutputStream, db: Db, field: AnyRef) =
		field match {
			case p: Proxy => out.writeBoolean(true) // for managed object, serialize object id
				val pc = db.proxyClass(p); val dbid = pc.dbid
				out.writeInt(dbid) ; out.writeUTF(pc.implements.getName)
			case o => out.writeBoolean(false) ; out.writeObject(o) // otherwise serialize normally
		}
	
	def rd[T](in: ObjectInputStream, db: Db) = {
		val res = (in.readBoolean() match {
			case false => in.readObject() // normal object
			case true => db.fromWeak(in.readInt, class4name(in.readUTF))
		}).asInstanceOf[T]
		assert(res != null); res
	}

}