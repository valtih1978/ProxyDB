import scala.language.reflectiveCalls, scala.language.postfixOps
import scala.collection.mutable, java.io._, Utils._

// Tree: The following command now succeeds
	// scala -J-Xmx33m ProxyDemo + 10 1m
// It fits into 33 mb of memory despite 2**10 2-mb strings
// take more than 2GB in main memory. In the latest version
// we partition both tree and string values that its nodes
// may have. It would be necessary (10+2) * 2 mb = 28 mb if
// we did not proxify the string values because final printng
// is recurisve and printin thread would not release
// the nodes whose methods are currently under invocation.
// Since depth of the tree is 10 nodes, all 10 nodes would be
// locked in mem. The cache also keeps two objects. However,
// since we know that tree values can be very large, we
// proxified them as well letting them go while thread descends
// into 10 small nodes.
 


object ProxyDemo extends App {

	if (args.length < 3) {
	
		println ("usage: <managed: + or -> <tree depth: int> <node size in unicode kilo chars> [scacheOff] [hcache<size>]")
		println ("scacheOff disables the soft cache for proxees")
		println ("example: + 3 3M scacheoff hcache100")
		println ("It will tree with (2^3)-1 = 7 nodes which will occupy 7 * 3 mln unicode chars = 21 mb")
		
	} else {
		
		val managed = args(0) == "+" //false
		val levels = args(1).toInt
		val a2 = args(2).toLowerCase ; val a2i = a2.filter { c => c.isDigit }.toInt
		val nodeSize = a2i * (?(a2.contains("k"), 1000, 1)) * (?(a2.contains("m"), 1000*1000, 1))
		
		def optArg(prefix: String) = args.drop(3).find { _.startsWith(prefix) }.map{_.replaceFirst(prefix, "")}
		cacheSize = optArg("hcache").map(_.toInt).getOrElse(2)
		withCleanDB{db =>
			if (optArg("scacheoff") != None){db.softCache = _ => null ; System.err.println("soft cache off")}
			//iterate(pairs) ; println("---") ; iterate(pairs)
		
			def gen(level: Int, cnt: Int): (Node, Int) = if (level == 0) (End, cnt) else {
				val (left, cnt2) = gen(level - 1, cnt)
				val (right, cnt3) = gen(level - 1, cnt2)
				//println(s"creating node $cnt3 at level $level")
				print(s"(n$cnt3:l$level)")
				def rnd = Array.fill(nodeSize)('1').mkString("")  // real random is too slow
				val value = Some(Value(s"$cnt3 $rnd")).map (value =>
					if (managed) db.put(value, classOf[IValue]) else value
				).get
				val tree = Some(Tree(value, left, right)).map (tree =>
					if (managed) db.put(tree, classOf[ITree]) else tree).get
				tree -> (cnt3 + 1)
				
			} ; val (tree, cnt) = gen(levels, 0)
			
			println(s"\nGenerated $cnt nodes. Taking into acccount " +
				s"cache of size $cacheSize, we need " +
				decimal((cnt + cacheSize) * nodeSize) + " bytes of memory to store all the strucutre")
				
			println("---") ;
			println(tree.str())
			
		}
	}
	
}


trait IValue // wraps a string into interface to
case class Value(value: String) extends IValue {
	override def toString = if (value.length < 20) value else value.hashCode + ""
	private def writeObject(oos: ObjectOutputStream) {
		//throw new Exception
		oos.defaultWriteObject
	}
}

trait Node extends Serializable {
	def str(indent: String = ""): String = "node"
}

object End extends Node
	
trait ITree extends Node with ProxifiableField {
	def value: IValue
	def left: Node ; def right: Node
	
	override def str(indent: String = ""): String = {
		s"\n$indent$value " + left.str(indent + " ") +
			"," + right.str(indent + " ")
	}
	
}


// We need to intorduce private variable coupled with def value
// for deserialization reasons http://stackoverflow.com/questions/33843884
case class Tree(@transient private var pValue: IValue, @transient private var pLeft: Node, @transient private var pRight: Node) extends ITree {

	def value = pValue
	def left = pLeft ; def right = pRight
	//println("created " + this)
	def writeFields(out: ObjectOutputStream, db: Db) {
		//out.defaultWriteObject() // serializes enclosing object
		Seq(value, left, right) foreach (wr(out, db, _))
	}
	def readFields(in: ObjectInputStream, db: Db) {
		//in.defaultReadObject()
		def rd[T] = super.rd[T](in, db)
		pValue = rd; pLeft = rd ; pRight = rd
	}
}


object PersistenceDemo extends App {

	def usage {
		println ("usage1: <#nodes to generate>")
		println ("usage2: load")
	}
	
	if (args.length < 1) usage else args(0) match {
		case "load" => withExistingDB{db =>
			println("loaded " + db.byName("root").toString())}
		case int => withCleanDB {db =>
			val len = args(0).toInt
			def rnd = scala.util.Random.alphanumeric.take(10).mkString
			val tree = (1 to len).foldLeft(End: Node) { case (tail, i) =>
				val value = db.put(Value(i + " " + rnd), classOf[IValue])
				db.put(Tree(value, tail, End), classOf[ITree])
			}
			val lastTreeId = db.name("root", tree)
			println("generated " + tree)
			println(" ---------- last tree id = " + lastTreeId)
		}
	}
}

object MutableDemo extends App {

	trait IList {
		override def toString = str(2) ; def str(depth: Int): String
		def next: IList ; def next_=(next: IList)
	}
	case class Cons(var value: String, @transient var next: IList) extends IList with ProxifiableField {
		
		// User logic
		def str (depth: Int) = {
			s"$value->" + ((next == null, depth == 1) match {
				case (true, _) => "null"
				case (_, true) => "..."
				case _ => next.str(depth-1)
			})
			
		}
		
		// handling proxifiable fields
		def writeFields(out: ObjectOutputStream, db: Db) =
									{wr(out, db, value) ; wr(out, db, next)}
		
		def readFields(in: ObjectInputStream, db: Db) =
									{value = rd(in, db) ; next = rd(in, db)}
		
		
	}
	
	def usage {
		println ("usage1: gen <ring size> <disaplay length>")
		println ("usage1: bug <ring size> <disaplay length>")
		println ("usage2: load <display length>")
		println ("example: gen 3 20")
	}

	def gen(useUpdateWrapper: Boolean) = {
		val ringLen = args(1).toInt
		closing(new Db(defaultDir, 0, true)){ db =>
			db.softCache = _ => null
			def manage(name: String, next: IList) = {
				db.put(Cons(name, next), classOf[IList])
			}
			val end = Cons("end1", null) ; val preEnd = manage("1", end)
			val head = (2 to args(1).toInt).foldLeft(preEnd){
				case(head, i) => manage(i + " ", head)
			}
			println("updating preEnd.next = " + preEnd.next + " to head = " + head)
			if (!useUpdateWrapper) preEnd.next = head
			else db.updating(preEnd){preEnd.next = head}
			println("in result, generated " + head.str(args(2).toInt))
			db.name("head", head)
		} ; closing(new Db(defaultDir, 0, false)){ db =>
			val end = (1 to ringLen).foldLeft(db.byName[IList]("head")){
				case(head, _) => head.next
			}
			println("end2 = " + end)
			assert(end.next != null, "BUG! BUG! BUG!\n"
				+ "BUG! BUG! BUG! You must forgotten to perform object update"
				+ "under updating(object) section and your updates were not"
				+ " saved to the disk.")
		}
	}
	if (args.length < 1) usage else args(0) match {
		case "bug" =>gen(false)
		case "gen" => gen(true)
		case "load" => withExistingDB{db =>
			val head: IList = db.byName("head")
			println(head.str(args(1) toInt))}
		case uncom => println("unknown command " + uncom) ; usage
	}
	
	def cast[T](o: Object) = o.asInstanceOf[T]
}