package tests;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;


import global.AttrOperator;
import global.AttrType;
import heap.HeapFile;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;
import relop.FileScan;
import relop.HashJoin;
import relop.Predicate;
import relop.Projection;
import relop.Schema;
import relop.Selection;
import relop.SimpleJoin;
import relop.Tuple;


public class QEPTest extends TestDriver {
    /**
     * The display name of the test suite.
     */
    private static final String TEST_NAME = "QEP Tests";

    private static QEPTest qepTest;

    /**
     * Employee table schema.
     */
    private static Schema s_emp;

    /**
     * Department table schema.
     */
    private static Schema s_dept;

    private static String empFilePath;
    private static String deptFilePath;

    private static HeapFile empHeapFile;
    private static HeapFile deptHeapFile;

    // --------------------------------------------------------------------------

    /**
     * Test application entry point; runs all tests.
     *
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        Result result = JUnitCore.runClasses(tests.QEPTest.class);

        for (Failure failure : result.getFailures()) {
            System.out.println(failure.toString());
        }

        System.out.println(result.wasSuccessful());
    }

    @BeforeClass
    public static void beforeClass() throws Exception {
        empFilePath = "Employee.txt";
        deptFilePath = "Department.txt";
        // create a clean Minibase instance
        qepTest = new QEPTest();
        qepTest.create_minibase();

        initSchema();
        loadData();
    }

    protected static void initSchema() {
        //initialize schema for the "Employee" table
        s_emp = new Schema(5);
        s_emp.initField(0, AttrType.INTEGER, 4, Constants.EmpIdColName);
        s_emp.initField(1, AttrType.STRING, 40, Constants.EmpNameColName);
        s_emp.initField(2, AttrType.FLOAT, 4, Constants.EmpAgeColName);
        s_emp.initField(3, AttrType.FLOAT, 4, Constants.EmpSalaryColName);
        s_emp.initField(4, AttrType.INTEGER, 4, Constants.EmpDeptIDColName);

        //initialize schema for the "Department" table
        s_dept = new Schema(4);
        s_dept.initField(0, AttrType.INTEGER, 4, Constants.DeptIdColName);
        s_dept.initField(1, AttrType.STRING, 40, Constants.DeptNameColName);
        s_dept.initField(2, AttrType.FLOAT, 4, Constants.DeptMinSalaryColName);
        s_dept.initField(3, AttrType.FLOAT, 4, Constants.DeptMaxSalaryColName);
    }

    protected static void loadData() throws IOException {
        empHeapFile = new HeapFile(null);
        loadEmpTable(empHeapFile);

        deptHeapFile = new HeapFile(null);
        loadDeptTable(deptHeapFile);
    }

    protected static void loadEmpTable(HeapFile file) throws IOException {
        Tuple tuple = new Tuple(s_emp);
        BufferedReader reader = new BufferedReader(new FileReader(empFilePath));
        reader.readLine(); //ignore the header
        String line = "";
        String[] temp = null;
        int empId;
        String empName;
        float age;
        float salary;
        int deptId;
        while ((line = reader.readLine()) != null) {
            temp = line.split(",");
            empId = Integer.parseInt(temp[0].trim());
            empName = temp[1].trim();
            age = Float.parseFloat(temp[2].trim());
            salary = Float.parseFloat(temp[3].trim());
            deptId = Integer.parseInt(temp[4].trim());
            tuple.setAllFields(empId, empName, age, salary, deptId);
            tuple.insertIntoFile(file);
        }
        reader.close();
    }

    protected static void loadDeptTable(HeapFile file) throws IOException {
        Tuple tuple = new Tuple(s_dept);
        BufferedReader reader = new BufferedReader(new FileReader(deptFilePath));
        reader.readLine(); //ignore the header
        String line = "";
        String[] temp = null;
        int deptId;
        String deptName;
        float minSalary;
        float maxSalary;
        while ((line = reader.readLine()) != null) {
            temp = line.split(",");
            deptId = Integer.parseInt(temp[0].trim());
            deptName = temp[1].trim();
            minSalary = Float.parseFloat(temp[2].trim());
            maxSalary = Float.parseFloat(temp[3].trim());
            tuple.setAllFields(deptId, deptName, minSalary, maxSalary);
            tuple.insertIntoFile(file);
        }
        reader.close();
    }

    @Test
    public void q1() {
        System.out.println("\n 1) Display for each employee his Name and Salary:\n");
        FileScan scan = new FileScan(s_emp, empHeapFile);
        Projection pro = new Projection(scan, s_emp.fieldNumber(Constants.EmpNameColName), s_emp.fieldNumber(Constants.EmpSalaryColName));
        pro.execute();
    }

    @Test
    public void q2() {
        System.out.println("\n 2) Display the Name for the departments with MinSalary = 1000:\n");
        FileScan scan = new FileScan(s_dept, deptHeapFile);
        Predicate pred = new Predicate(AttrOperator.EQ, AttrType.COLNAME, Constants.DeptMinSalaryColName, AttrType.FLOAT, 1000F);
        Selection sel = new Selection(scan, pred);
        Projection pro = new Projection(sel, s_dept.fieldNumber(Constants.DeptNameColName));
        pro.execute();
    }

    @Test
    public void q3() {
        System.out.println("\n 3) Display the Name for the departments with MinSalary = MaxSalary:\n");
        FileScan scan = new FileScan(s_dept, deptHeapFile);
        Predicate pred = new Predicate(AttrOperator.EQ, AttrType.COLNAME, Constants.DeptMinSalaryColName, AttrType.COLNAME, Constants.DeptMaxSalaryColName);
        Selection sel = new Selection(scan, pred);
        Projection pro = new Projection(sel, s_dept.fieldNumber(Constants.DeptNameColName));
        pro.execute();
    }

    @Test
    public void q4() {
        System.out.println("\n4) Display the Name for employees whose Age > 30 and Salary < 1000:\n");
        FileScan scan = new FileScan(s_emp, empHeapFile);

        // Create predicates for Salary < 1000 and Age > 30
        Predicate predicate = new Predicate(AttrOperator.LT, AttrType.COLNAME, Constants.EmpSalaryColName, AttrType.FLOAT, 1000F);
        Predicate predicate2 = new Predicate(AttrOperator.GT, AttrType.COLNAME, Constants.EmpAgeColName, AttrType.FLOAT, 30F);

        // Apply selections using predicates
        Selection selection = new Selection(scan, predicate);
        Selection selection2 = new Selection(selection, predicate2);

        // Create projection
        Projection projection = new Projection(selection2, s_emp.fieldNumber(Constants.EmpNameColName));
        projection.execute();
    }

    @Test
    public void q5() {
        System.out.println("\n 5) For each employee, display his Salary and the Name of his department:\n");
        FileScan empScan = new FileScan(s_emp, empHeapFile);
        FileScan deptScan = new FileScan(s_dept, deptHeapFile);
        Predicate joinPred = new Predicate(AttrOperator.EQ, AttrType.FIELDNO, 0, AttrType.FIELDNO, 8);
        SimpleJoin join = new SimpleJoin(deptScan, empScan, joinPred);
        Projection proj = new Projection(join, 7, 1);
        proj.execute();
    }

    @Test
    public void q6() {
        System.out.println("\n6) Display the Name and Salary for employees who work in the department that has DeptId = 3:\n");
        FileScan empScan = new FileScan(s_emp, empHeapFile);

        // Create a predicate to filter employees with DeptId = 3
        Predicate deptPredicate = new Predicate(AttrOperator.EQ, AttrType.COLNAME, Constants.DeptIdColName, AttrType.INTEGER, 3);

        // Apply selection using predicate
        Selection selection = new Selection(empScan, deptPredicate);

        // Project the Name and Salary
        Projection projection = new Projection(
                selection,
                s_emp.fieldNumber(Constants.EmpNameColName),   // Name field
                s_emp.fieldNumber(Constants.EmpSalaryColName) // Salary field
        );

        projection.execute();
    }

    @Test
    public void q7() {
        System.out.println("\n 7) Display the Salary for each employee who works in a department that has MaxSalary > 100000:\n");

        FileScan empScan = new FileScan(s_emp, empHeapFile);
        FileScan deptScan = new FileScan(s_dept, deptHeapFile);

        // Select departments with MaxSalary > 100000
        Predicate predicate = new Predicate(AttrOperator.GT, AttrType.COLNAME, Constants.DeptMaxSalaryColName, AttrType.FLOAT, 100000F);
        Selection selection = new Selection(deptScan, predicate);

        // Join Employee and Department tables on DeptID
        Predicate join_predicate = new Predicate(AttrOperator.EQ, AttrType.FIELDNO, 4, AttrType.FIELDNO, 0);
        SimpleJoin join = new SimpleJoin(empScan, selection, join_predicate);

        // Project the Employee Salary
        Projection projection = new Projection(join, s_emp.fieldNumber(Constants.EmpSalaryColName));
        projection.execute();
    }

    @Test
    public void q8() {
        System.out.println("\n8) Display the Name for each employee whose Salary is less than the MinSalary of their department:\n");
        FileScan empScan = new FileScan(s_emp, empHeapFile);
        FileScan deptScan = new FileScan(s_dept, deptHeapFile);

        // Join Employee and Department tables on DeptID
        HashJoin join = new HashJoin(empScan, deptScan, 4, 0);

        // Select records where Employee Salary < Department MinSalary
        Predicate predicate = new Predicate(
                AttrOperator.LT,
                AttrType.FIELDNO, s_emp.fieldNumber(Constants.EmpSalaryColName),  // Employee.Salary (FLOAT)
                AttrType.FIELDNO, s_dept.fieldNumber(Constants.DeptMinSalaryColName) // Department.MinSalary (FLOAT)
        );
        Selection selection = new Selection(join, predicate);

        // Project the Employee Name
        Projection projection = new Projection(selection, s_emp.fieldNumber(Constants.EmpNameColName));

        // Execute the query
        projection.execute();
    }

    public void q4_hash() //hash join version of query 4
    {
        System.out.println("\n 5) For each employee, display his Salary and the Name of his department:\n");
        FileScan empScan = new FileScan(s_emp, empHeapFile);
        FileScan deptScan = new FileScan(s_dept, deptHeapFile);
        Predicate joinPred = new Predicate(AttrOperator.EQ, AttrType.FIELDNO, 0, AttrType.FIELDNO, 8);
        //SimpleJoin join = new SimpleJoin(deptScan, empScan, joinPred);
        HashJoin join = new HashJoin(deptScan, empScan, 0, 4);
        Projection proj = new Projection(join, 7, 1);
        proj.execute();
    }

    static class Constants {
        public final static String EmpIdColName = "EmpId";
        public final static String EmpNameColName = "Name";
        public final static String EmpAgeColName = "Age";
        public final static String EmpSalaryColName = "Salary";
        public final static String EmpDeptIDColName = "DeptID";

        public final static String DeptIdColName = "DeptId";
        public final static String DeptNameColName = "Name";
        public final static String DeptMinSalaryColName = "MinSalary";
        public final static String DeptMaxSalaryColName = "MaxSalary";
    }
}
