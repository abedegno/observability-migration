// Copyright 2026 obs-migrate contributors. SPDX-License-Identifier: Apache-2.0
//! DataFusion FFI plan; native parsing remains in the existing Python readers.
use arrow::{
    array::Array,
    datatypes::{DataType, Schema, SchemaRef},
    pyarrow::FromPyArrow,
    record_batch::RecordBatch,
};
use async_trait::async_trait;
use datafusion::{
    catalog::{Session, TableProvider},
    common::{DataFusionError, Result, ScalarValue, project_schema, tree_node::TreeNodeRecursion},
    execution::TaskContext,
    logical_expr::{Expr, Operator, TableProviderFilterPushDown, TableType},
    physical_expr::{EquivalenceProperties, PhysicalExpr},
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
        RecordBatchStream, SendableRecordBatchStream,
        execution_plan::{Boundedness, EmissionType},
    },
};
use datafusion_ffi::{
    proto::logical_extension_codec::FFI_LogicalExtensionCodec, table_provider::FFI_TableProvider,
};
use futures::Stream;
use pyo3::{
    exceptions::{PyStopIteration, PyValueError},
    prelude::*,
    types::PyCapsule,
};
use std::{
    fmt,
    pin::Pin,
    ptr::NonNull,
    sync::Arc,
    task::{Context, Poll},
};

// Namespace, byte key, byte value. This protocol is versioned with the bridge.
type Clauses = Vec<(String, Vec<u8>, Vec<u8>)>;

fn binary(expr: &Expr) -> Option<&Vec<u8>> {
    match expr {
        Expr::Literal(
            ScalarValue::Binary(Some(v))
            | ScalarValue::LargeBinary(Some(v))
            | ScalarValue::BinaryView(Some(v)),
            _,
        ) => Some(v),
        _ => None,
    }
}

fn bounded(namespace: &str, key: &[u8], value: &[u8]) -> Clauses {
    if key.is_empty()
        || 1 + key.len() + 2 + value.len() > 2047
        || (namespace == "dimension" && key.windows(2).any(|w| w == b"::"))
    {
        return vec![];
    }
    vec![(namespace.into(), key.to_vec(), value.to_vec())]
}

fn metric_name(expr: &Expr) -> bool {
    let Expr::ScalarFunction(field) = expr else {
        return false;
    };
    if field.name() != "get_field"
        || field.args.len() != 2
        || !matches!(&field.args[1], Expr::Literal(ScalarValue::Utf8(Some(name)), _) if name == "name")
    {
        return false;
    }
    let Expr::ScalarFunction(element) = &field.args[0] else {
        return false;
    };
    element.name() == "array_element"
        && element.args.len() == 2
        && matches!(&element.args[0], Expr::Column(c) if c.name == "measurements")
        && matches!(
            &element.args[1],
            Expr::Literal(ScalarValue::Int64(Some(1)), _)
        )
}

fn dimension_type(data_type: &DataType) -> bool {
    // DataFusion adds a cast solely to reconcile struct child nullability with
    // named_struct literals. Accept only the exact binary key/value shape.
    let DataType::List(element) = data_type else {
        return false;
    };
    let DataType::Struct(fields) = element.data_type() else {
        return false;
    };
    fields.len() == 2
        && fields[0].name() == "key"
        && fields[1].name() == "value"
        && fields.iter().all(|f| f.data_type() == &DataType::Binary)
}

fn dimensions(expr: &Expr) -> bool {
    match expr {
        Expr::Column(c) => c.name == "dimensions",
        Expr::Cast(c) => {
            matches!(&*c.expr, Expr::Column(col) if col.name == "dimensions")
                && dimension_type(c.field.data_type())
        }
        _ => false,
    }
}

fn indexed_field(expr: &Expr, column: &str, name: &str) -> bool {
    matches!(expr, Expr::ScalarFunction(f) if f.name() == "get_field" && f.args.len() == 2
        && matches!(&f.args[0], Expr::Column(c) if c.name == column)
        && matches!(&f.args[1], Expr::Literal(ScalarValue::Utf8(Some(v)), _) if v == name))
}

fn decimal_field(expr: &Expr, column: &str, name: &str) -> bool {
    matches!(expr, Expr::Cast(c) if c.field.data_type() == &DataType::Decimal128(30, 9)
        && indexed_field(&c.expr, column, name))
}

fn exact_timestamp(expr: &Expr, column: &str) -> bool {
    matches!(expr, Expr::BinaryExpr(b) if b.op == Operator::Plus
        && ((decimal_field(&b.left, column, "seconds") && decimal_field(&b.right, column, "fraction"))
            || (decimal_field(&b.right, column, "seconds") && decimal_field(&b.left, column, "fraction"))))
}

fn time_clause(
    field: &Expr,
    literal: &Expr,
    op: Operator,
    column: &str,
    namespace: &str,
) -> Clauses {
    let key = match op {
        Operator::Eq => "eq",
        Operator::Gt => "gt",
        Operator::GtEq => "ge",
        Operator::Lt => "lt",
        Operator::LtEq => "le",
        _ => return vec![],
    };
    let ns = 1_000_000_000_i128;
    if indexed_field(field, column, "seconds")
        && let Expr::Literal(ScalarValue::Int64(Some(seconds)), _) = literal
    {
        let start = i128::from(*seconds) * ns;
        // Integer-second SQL filters include/exclude the *whole* second,
        // including its fractional points. Avoid endpoint +1 overflow.
        let pairs = if column == "indexed_time" {
            // Preserve the metric nanosecond-only contract and old protocol.
            match op {
                Operator::Eq => vec![("ge", start), ("le", start + ns - 1)],
                Operator::Gt => vec![("gt", start + ns - 1)],
                Operator::LtEq => vec![("le", start + ns - 1)],
                _ => vec![(key, start)],
            }
        } else {
            // Event fractions can extend below nanoseconds. Use an exclusive
            // next-second endpoint so .999999999<nonzero digits> stays inside.
            // Out-of-Int64-second endpoints are ignored by the event backend;
            // every expression remains residual and cannot lose matching rows.
            match op {
                Operator::Eq => vec![("ge", start), ("lt", start + ns)],
                Operator::Gt => vec![("ge", start + ns)],
                Operator::LtEq => vec![("lt", start + ns)],
                _ => vec![(key, start)],
            }
        };
        return pairs
            .into_iter()
            .map(|(k, v)| {
                (
                    namespace.into(),
                    k.as_bytes().to_vec(),
                    v.to_string().into_bytes(),
                )
            })
            .collect();
    }
    if exact_timestamp(field, column)
        && let Expr::Literal(ScalarValue::Decimal128(Some(value), 31, 9), _) = literal
    {
        let seconds = value.div_euclid(ns);
        if seconds >= i128::from(i64::MIN) && seconds <= i128::from(i64::MAX) {
            return vec![(
                if column == "indexed_time" {
                    namespace.into()
                } else {
                    format!("{namespace}:decimal9")
                },
                key.as_bytes().to_vec(),
                value.to_string().into_bytes(),
            )];
        }
    }
    vec![]
}

/// Recognize only necessary AND factors over the private session's built-in
/// functions. OR/NOT, arbitrary casts/functions and other shapes stay residual.
fn clauses(expr: &Expr) -> Clauses {
    if let Expr::BinaryExpr(b) = expr {
        if b.op == Operator::And {
            let mut out = clauses(&b.left);
            out.extend(clauses(&b.right));
            return out;
        }
        for (column, namespace) in [
            ("indexed_time", "time"),
            ("event_time", "event_time"),
            ("index_time", "index_time"),
        ] {
            let time = time_clause(&b.left, &b.right, b.op, column, namespace);
            if !time.is_empty() {
                return time;
            }
            if let Some(reversed) = b.op.swap() {
                let time = time_clause(&b.right, &b.left, reversed, column, namespace);
                if !time.is_empty() {
                    return time;
                }
            }
        }
        if b.op == Operator::Eq {
            for (field, literal) in [(&*b.left, &*b.right), (&*b.right, &*b.left)] {
                let Some(value) = binary(literal) else {
                    continue;
                };
                if let Expr::Column(column) = field
                    && matches!(column.name.as_str(), "host" | "source" | "sourcetype")
                {
                    return bounded("field", column.name.as_bytes(), value);
                }
                // Equality on the first measurement implies membership of its
                // name. The residual still verifies position and SQL semantics.
                if metric_name(field) {
                    return bounded("field", b"metric_name", value);
                }
            }
        }
    }
    if let Expr::ScalarFunction(f) = expr
        && f.name() == "array_has"
        && f.args.len() == 2
        && dimensions(&f.args[0])
        && let Expr::Literal(ScalarValue::Struct(pair), _) = &f.args[1]
    {
        if pair.len() != 1
            || pair.is_null(0)
            || pair.num_columns() != 2
            || pair.fields()[0].name() != "key"
            || pair.fields()[1].name() != "value"
        {
            return vec![];
        }
        let key = ScalarValue::try_from_array(pair.column(0), 0);
        let value = ScalarValue::try_from_array(pair.column(1), 0);
        if let (Ok(ScalarValue::Binary(Some(k))), Ok(ScalarValue::Binary(Some(v)))) = (key, value) {
            return bounded("dimension", &k, &v);
        }
    }
    vec![]
}

struct Provider {
    schema: SchemaRef,
    backend: Arc<Py<PyAny>>,
}
impl fmt::Debug for Provider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("NativeCaptureProvider")
    }
}
#[async_trait]
impl TableProvider for Provider {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn table_type(&self) -> TableType {
        TableType::Base
    }
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|e| {
                if clauses(e).is_empty() {
                    TableProviderFilterPushDown::Unsupported
                } else {
                    TableProviderFilterPushDown::Inexact
                }
            })
            .collect())
    }
    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let schema = project_schema(&self.schema, projection)?;
        let mut pushed = vec![];
        for filter in filters {
            pushed.extend(clauses(filter));
        }
        pushed.sort();
        pushed.dedup();
        let projection = projection
            .cloned()
            .unwrap_or_else(|| (0..self.schema.fields().len()).collect());
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Ok(Arc::new(NativeExec {
            schema,
            properties,
            backend: self.backend.clone(),
            pushed,
            projection,
        }))
    }
}

struct NativeExec {
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
    backend: Arc<Py<PyAny>>,
    pushed: Clauses,
    projection: Vec<usize>,
}
impl fmt::Debug for NativeExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.fmt_as(DisplayFormatType::Default, f)
    }
}
impl DisplayAs for NativeExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "NativeCaptureExec: projection={:?}, pushed_equalities={}, residuals=retained",
            self.projection,
            self.pushed.len()
        )
    }
}
impl ExecutionPlan for NativeExec {
    fn name(&self) -> &str {
        "NativeCaptureExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }
    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return Err(DataFusionError::Plan("native scan has no children".into()));
        }
        Ok(self)
    }
    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Execution(
                "native scan has one partition".into(),
            ));
        }
        Ok(Box::pin(PyStream {
            schema: self.schema.clone(),
            backend: self.backend.clone(),
            pushed: self.pushed.clone(),
            projection: self.projection.clone(),
            iterator: None,
            done: false,
        }))
    }
}

struct PyStream {
    schema: SchemaRef,
    backend: Arc<Py<PyAny>>,
    pushed: Clauses,
    projection: Vec<usize>,
    iterator: Option<Py<PyAny>>,
    done: bool,
}
impl Stream for PyStream {
    type Item = Result<RecordBatch>;
    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.done {
            return Poll::Ready(None);
        }
        let result = Python::attach(|py| -> PyResult<Option<RecordBatch>> {
            if this.iterator.is_none() {
                this.iterator = Some(
                    this.backend
                        .bind(py)
                        .call_method1("scan", (this.pushed.clone(), this.projection.clone()))?
                        .unbind(),
                );
            }
            match this
                .iterator
                .as_ref()
                .unwrap()
                .bind(py)
                .call_method0("__next__")
            {
                Ok(value) => {
                    let batch = RecordBatch::from_pyarrow_bound(&value)?;
                    if batch.schema() != this.schema {
                        return Err(PyValueError::new_err("native Arrow batch schema mismatch"));
                    }
                    Ok(Some(batch))
                }
                Err(error) if error.is_instance_of::<PyStopIteration>(py) => Ok(None),
                Err(error) => Err(error),
            }
        });
        Poll::Ready(match result {
            Ok(Some(batch)) => Some(Ok(batch)),
            Ok(None) => {
                this.done = true;
                None
            }
            Err(error) => {
                this.done = true;
                Some(Err(DataFusionError::Execution(format!(
                    "native capture stream: {error}"
                ))))
            }
        })
    }
}
impl RecordBatchStream for PyStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
impl Drop for PyStream {
    fn drop(&mut self) {
        if let Some(iterator) = self.iterator.take() {
            Python::attach(|py| {
                let _ = iterator.bind(py).call_method0("close");
            });
        }
    }
}

#[pyclass]
struct NativeProvider {
    inner: Arc<Provider>,
}
#[pymethods]
impl NativeProvider {
    #[new]
    fn new(schema: &Bound<'_, PyAny>, backend: Py<PyAny>) -> PyResult<Self> {
        let schema = Arc::new(Schema::from_pyarrow_bound(schema)?);
        Ok(Self {
            inner: Arc::new(Provider {
                schema,
                backend: Arc::new(backend),
            }),
        })
    }
    fn __datafusion_table_provider__<'py>(
        &self,
        py: Python<'py>,
        session: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let value = session.call_method0("__datafusion_logical_extension_codec__")?;
        let capsule = value.cast::<PyCapsule>()?;
        let ptr: NonNull<FFI_LogicalExtensionCodec> = capsule
            .pointer_checked(Some(c"datafusion_logical_extension_codec"))?
            .cast();
        // The host supplies this typed, name-checked FFI capsule. Never accept
        // arbitrary pointers or private DataFusion memory layouts.
        let codec = unsafe { ptr.as_ref() };
        if unsafe { (codec.version)() } != 55 {
            return Err(PyValueError::new_err("DataFusion FFI major must be 55"));
        }
        let provider =
            FFI_TableProvider::new_with_ffi_codec(self.inner.clone(), true, None, codec.clone());
        PyCapsule::new_with_value(py, provider, cr"datafusion_table_provider")
    }
}

#[pymodule]
fn _native_provider(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<NativeProvider>()?;
    module.add("BRIDGE_REVISION", "obs-native-ffi-3")?;
    module.add("EVENT_TIME_CLAUSES_REVISION", 1)?;
    module.add(
        "BUILD_PROFILE",
        if cfg!(debug_assertions) {
            "dev"
        } else {
            "release"
        },
    )?;
    module.add("DATAFUSION_RUST_VERSION", "55.2.0")?;
    module.add("ARROW_RUST_VERSION", "59.3.0")?;
    Ok(())
}
