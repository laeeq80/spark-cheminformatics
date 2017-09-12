package se.uu.farmbio.parsers

import java.io.File
import scala.collection.mutable.ListBuffer
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.FileSplit
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.Reporter
import org.junit.runner.RunWith
import org.mockito.Mockito.mock
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class PDBRecordReaderTest extends FunSuite {

  test("PDBRecordReader should parse a PDB file correctly, according to size parameter") {

    //Make some mocks
    val size = 3
    val jobConf = new JobConf()
    jobConf.set(PDBRecordReader.SIZE_PROPERTY_NAME, size.toString)
    val standardFile = new File(getClass.getResource("filtered_conformers.pdbqt").getPath())
    val standardFileSplit = new FileSplit(new Path(standardFile.getPath()), 0, standardFile.length, jobConf)

    //Perform test
    val inputFormat = new PDBInputFormat
    val recordReader = inputFormat.getRecordReader(standardFileSplit, jobConf, mock(classOf[Reporter]))
    val toTest = new ListBuffer[String]
    var key = recordReader.createKey()
    var value = recordReader.createValue()
    while (recordReader.next(key, value)) {
      val values = value.toString().split("ENDMDL")
      values.foreach(toTest += _.trim + "\nENDMDL")
      key = recordReader.createKey()
      value = recordReader.createValue()
      assert(values.length == size ||
        (values.length < size && !recordReader.next(key, value)))
    }
    val standardList = TestUtils.readPDB(
      getClass.getResource("filtered_conformers.pdbqt").getPath)
    assert(toTest === standardList)

  }

}