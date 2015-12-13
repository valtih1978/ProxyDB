import java.lang.reflect.{Method, InvocationHandler, Proxy}
import scala.language.{existentials, reflectiveCalls, postfixOps}
import java.nio._, java.io._, java.nio.channels._
import Utils._, scala.collection.mutable

// results are noticeably worse, especially when I used Scala reflection in the beginning.
// I have got => better results after factored out the common part and then after switching 
// to Java Reflection.

//JAVA_HOME=c:\Program Files (x86)\Java\jdk1.8.0_31:
// timeit "scala -J-Xmx33m ProxyDemo + 16 100 > nul // 41 sec => 33 => 25
// timeit "scala -J-Xmx33m ProxyDemo + 16 100 scacheoff > nul // 31 sec => 22 => 17
// timeit "scala -J-Xmx1000m ProxyDemo + 18 1k > nul" // 79 => 60 => 51
// timeit "scala -J-Xmx1000m ProxyDemo + 18 1k scacheoff > nul" // 144 => 106 => 87

//java_home=c:\Program Files\java\jre1.8.0_45
// timeit "scala -J-Xmx33m ProxyDemo + 16 100 > nul // 35 => 32 => 30
// timeit "scala -J-Xmx33m ProxyDemo + 16 100 scacheoff > nul // 18 => 14 => 12
// timeit "scala -J-Xmx1000m ProxyDemo + 18 1k > nul" // 42 => 36 => 31
// timeit "scala -J-Xmx1000m ProxyDemo + 18 1k scacheoff > nul" // 70 => 57 => 52

class Db(dir: File, var cacheSize: Int, clean: Boolean) {
	
	import java.lang.ref._
	
	System.err.println(s"hard cache size $cacheSize objects, dir = $dir")
	
	var raf: RandomAccessFile = _ ; var rafCh: FileChannel = _
	var physicalIndex: RafIndex = _
	
	// We probably do not need this since soft references are employed.
	val hardCache = mutable.Queue[Proxy]()
	
	// I would like to merge the ProxyClass with WeakReference but
	// constructor of weakref needs a proxy, which needs proxy class
	// for its constructor.
	// When proxee != null in constructor, must set drity = Yes and call activate. This will force serialization on eviction.
	class ProxyClass (proxy: Proxy,
		val dbid: Int, var proxee: Object, //  not null proxees must be exacly in the actives
		val implements: Class[_],
		var dirty: Boolean

	//class HardReference[T](val referent: Proxy, rq1: Object) extends SoftReference[Proxy](referent, rq)
	//class MyWR(val dbid: Int, proxy: Proxy) extends WeakReference[Proxy](proxy, rq)
	
		) extends WeakReference(proxy, rq) with InvocationHandler { rqClean
	
	// Physical ID might be used to support mutability. When object
	// is updated, a new revision is created. In append-only DB,
	// physical ID may be the offset of the serialized object
		var physicalID: Long = -1// use phys id -1 for `not initialized`. It is -1 when new object added or not yet loaded
	
		//println("creating " + this)
		
		// When new object is created we may evict something from mem
		
	  override def invoke(proxy: Object, method: Method, args: Array[Object]): AnyRef = {

			//println("calling1 " + this +"."+ method.getName + ", victims = " + victims)
			assert(weakProxies(dbid) == this, s"invoking $this over finalized proxy!") ;	assert(get == proxy, this + " proxy in the weak map mismatches the invoked one")
			//assert(wr.get eq proxy, this + s" is accessed through ${System.identityHashCode(proxy)} but is known as ${wr.get} in our weakMap")
			
			proxee = proxee match {
				case ref: Reference[Object] => ref.get //val res = ref.get ; if (res != null) println(s"restored $this from memory") ; res
				case a => a
			}
			
			if (proxee == null) {
				if (physicalID == -1) physicalID = physicalIndex(dbid)//pass(physicalIndex(dbid)){p => println(s"restoring $this from unknown disk location, which happens to be $p") ; physicalID = p}
				//else println(s"restoring $this from known disk location")
				rafCh.position(physicalID) //; println("raf ch pos set to " + rafCh.position)
				val in = new ObjectInputStream(Channels.newInputStream(rafCh))
				proxee = in.readObject ; proxee match {
					case ts: ProxifiableField => ts.readFields(in, Db.this) ; ts
					case o => o // this is conventional object
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
			
			/*closing(new ObjectOutputStream(new OutputStream {
				def write(b: Int) = raf.writeByte(b)
				def write(buf: Array[Byte], off: Int, len: Int) = raf.write(buf, off, len)
				def write(buf: Array[Byte]) = raf.write(buf)
			}))*/
			physicalID = raf.length; rafCh.position(physicalID)
			val oos = new ObjectOutputStream(Channels.newOutputStream(rafCh))
			oos.writeObject(proxee) ; proxee match {
					case ts: ProxifiableField => ts.writeFields(oos, Db.this)
					case _ => // this is conventional object
			} ; oos.flush
			physicalIndex(dbid) = physicalID ; dirty = false
			//println("after write, raf len = " + raf.length)
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
	val weakProxies: mutable.Map[Int, ProxyClass] = mutable.Map()
	
	var total: Int = _
	
	//import scala.reflect.runtime.universe._
	//def getType[T: TypeTag](a: T): Type = typeOf[T]
	def put[T <: AnyRef with Serializable, I](proxee: T, supportedInterfaces: Class[I]) = {
		total += 1 ;
		val proxy = makeProxy(total, proxee, supportedInterfaces, true).asInstanceOf[Proxy]
		activate(proxy) ; proxy.asInstanceOf[I] // need to activate coz deactivation serializes the object
	}

	
	private val rq = new ReferenceQueue[Proxy]() ; var finalizedPC = 0
	@annotation.tailrec final def rqClean {rq.poll match {
		case null =>
		case top =>	finalizedPC += 1; val dbid = top.asInstanceOf[ProxyClass].dbid
			assert(weakProxies.contains(dbid)) ; val current = weakProxies(dbid)
			//println((if (current != top) "a previous copy of " else "") + s"proxy with dbid " + dbid + " was finalized")
			if (current == top) weakProxies -= dbid ; rqClean
	}}
	
	object FakeIH extends InvocationHandler {
	  override def invoke(proxy: Object, method: Method, args: Array[Object]): AnyRef = {
	  	throw new Exception("this handler is supposed to be replaced by weak reference rather than called.")
	  }
	}
	
	//pc must have proxee initialized
	def makeProxy(dbid: Int, proxee: Object, implements: Class[_], dirty: Boolean) = {
		assert(!weakProxies.contains(dbid) || weakProxies(dbid).get == null)
		val proxy = Proxy.newProxyInstance(implements.getClassLoader, Array(implements), FakeIH).asInstanceOf[Proxy]
		val pc = new ProxyClass(proxy, dbid, proxee, implements, dirty)
   	weakProxies(dbid) = pc ; updateInvocationHandler(proxy, pc)
   	proxy
	}
	
 	def fname(name: String) = new File(pname(name))
	def pname(name: String) = dir + "/" + name
	
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
		
	val physicalIndexPath = pname("physicalid.longs")
	
	def close() {
		rqClean ; System.err.println(s"finalized + " + decimal(finalizedPC) + ", number of proxies remaining known to the db = " + weakProxies.size)
		flush ; hardCache.clear ; names.clear ; physicalIndex(0) = total
		physicalIndex.close ; rafCh.close; raf.close
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
		
		physicalIndex = new MmfIndex(physicalIndexPath) ; total = physicalIndex(0).toInt
		raf = new RandomAccessFile(pname("objects.bin"), "rw") ; rafCh = raf.getChannel
		
	}
	def restart() = { close ; open }

	if (clean) {
	 	dir.mkdir()
	 	dir.listFiles().foreach { f => file.Files.deleteIfExists(f.toPath())}
	 	assert(dir.list().length == 0)
	 	fNames.createNewFile() ; val total = 0
	 	closing(new RafIndex(physicalIndexPath)){_.append(total)} // reset total to 0
	}
	
	open()

	def fromWeak(dbid: Int, implements: Class[_]) = {
		weakProxies.get(dbid) map {_.get} filter(_!= null) getOrElse {
			makeProxy(dbid, null, implements, false)
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