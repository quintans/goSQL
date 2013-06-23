package db

import (
	"github.com/quintans/goSQL/dbx"
	"github.com/quintans/toolkit/log"
)

var logger = log.LoggerFor("github.com/quintans/goSQL/db")

func init() {
	// activates output of program file line
	logger.CallDepth(1)
}

var OPTIMISTIC_LOCK_MSG = "No update was possible for this version of the data. Data may have changed."
var VERSION_SET_MSG = "Unable to set Version data."

type operation int

const (
	opInsert operation = iota
	opUpdate
	opDelete
)

type IDb interface {
	GetTranslator() Translator
	GetConnection() dbx.IConnection
	Query(table *Table) *Query
	Insert(table *Table) *Insert
	Delete(table *Table) *Delete
	Update(table *Table) *Update
}

var _ IDb = &Db{}

func NewDb(connection dbx.IConnection, translator Translator) *Db {
	return &Db{connection, translator}
}

type Db struct {
	connection dbx.IConnection
	translator Translator
}

func (this *Db) GetTranslator() Translator {
	return this.translator
}

func (this *Db) GetConnection() dbx.IConnection {
	return this.connection
}

// the idea is to centralize the query creation so that future customization could be made
func (this *Db) Query(table *Table) *Query {
	return NewQuery(this, table)
}

// the idea is to centralize the query creation so that future customization could be made
func (this *Db) Insert(table *Table) *Insert {
	return NewInsert(this, table)
}

// the idea is to centralize the query creation so that future customization could be made
func (this *Db) Delete(table *Table) *Delete {
	return NewDelete(this, table)
}

// the idea is to centralize the query creation so that future customization could be made
func (this *Db) Update(table *Table) *Update {
	return NewUpdate(this, table)
}

// >>>>>>>>>>>>>

/*


	public Driver getDriver() {
		return this.driver;
	}

	public void setDriver(Driver driver) {
		this.driver = driver;
	}

	public SimpleJdbc getSimpleJdbc() {
		return this.simpleJdbc;
	}

	public Long fetchAutoNumber(Column column) {
		return fetchAutoNumber(column, false);
	}

	public Long fetchCurrentAutoNumber(Column column) {
		return fetchAutoNumber(column, true);
	}

	public Long fetchAutoNumber(Column column, boolean current) {
		String sql = current ? this.driver.getCurrentAutoNumberQuery(column) : this.driver.getAutoNumberQuery(column);
		logger.debug("SQL: " + sql);
		long now = System.currentTimeMillis();
		Long id = this.simpleJdbc.queryForLong(getConnection(), sql, new LinkedHashMap<String, Object>());
		logger.debug("executado em: " + (System.currentTimeMillis() - now) + "ms");
		return id;
	}

	@SuppressWarnings("unchecked")
	public Map<Column, Object> insert(Table table, Object bean) {
		return (Map<Column, Object>) crud(table, bean, Operation.INSERT);
	}


	public int update(Table table, Object bean) {
		return (Integer) crud(table, bean, Operation.UPDATE);
	}

	public int delete(Table table, Object bean) {
		return (Integer) crud(table, bean, Operation.DELETE);
	}

	// NOTA: este método tem que levar em consideração colunas VERSION
	protected Object crud(Table table, Object bean, Operation operation) {
		Object result = null;
		if (bean != null) {
			DmlCore<?> mainDml = null;
			switch (operation) {
			case INSERT:
				mainDml = createInsert(table);
				break;

			case UPDATE:
				mainDml = createUpdate(table);
				break;

			case DELETE:
				mainDml = createDelete(table);
				break;
			}

			boolean hasVirtual = false;
			boolean hasVersion = false;

			Map<String, BeanProperty> mappings = BeanProperty.populateMapping(null, bean.getClass());
			List<Condition> conditions = new ArrayList<Condition>();
			BeanProperty versionBeanProperty = null;
			Long versionValue = null;


			// cria a operação principal
			for (Column column : table.getColumns()) {
				if (column.isVirtual())
					hasVirtual = true;
				else {
					BeanProperty bp = mappings.get(column.getAlias());
					if (bp != null) {
						Object o = null;
						try {
							o = bp.getReadMethod().invoke(bean);
						} catch (Exception e) {
							// e.printStackTrace();
						}

						if (operation == Operation.INSERT) {
							if (column.isVersion() && o == null) {
								try {
									bp.getWriteMethod().invoke(bean, 1L);
								} catch (Exception e) {
									// e.printStackTrace();
								}
								mainDml.set(column, 1L);
							} else
								mainDml.set(column, o);

							if (column.getDiscriminator() != null)
								mainDml.value(column, column.getDiscriminator());

						} else {
							if (column.isKey()) {
								if (o == null)
									throw new PersistenceException(String.format("Value for key property '%s' cannot be null.", column.getAlias()));

								conditions.add(column.matches(param(column.getAlias())));
								mainDml.setParameter(column, o);
							} else if (column.isVersion()) {
								// if version is null ignores it
								if (o != null) {
									Long version = ((Number) o).longValue();
									String alias = "_" + column.getAlias() + "_";
									conditions.add(column.matches(param(alias)));
									mainDml.setInteger(alias, version);

									// version increment
									versionValue = version + 1;
									mainDml.set(column, versionValue);

									hasVersion = true;
									versionBeanProperty = bp;
								}
							} else if (operation == Operation.UPDATE) {
								mainDml.set(column, o);

								if (column.getDiscriminator() != null)
									mainDml.value(column, column.getDiscriminator());
							}
						}
					}
				}
			}

			// executa a operação principal
			if (operation == Operation.INSERT) {
				Map<Column, Object> keys = ((Insert) mainDml).execute();
				if (keys != null) {
					for (Entry<Column, Object> entry : keys.entrySet()) {
						Column col = entry.getKey();
						Object val = entry.getValue();
						// to be used below
						if (val != null) // can be null if it wasn't auto-generated
							mainDml.setParameter(col, val);

						// update bean key properties
						BeanProperty bp = mappings.get(col.getAlias());
						if (bp != null) {
							try {
								bp.getWriteMethod().invoke(bean, val);
							} catch (Exception e) {
								// e.printStackTrace();
							}
						}
					}
				}
				result = keys;
			} else {
				if (conditions.size() > 0)
					((Dml<?>) mainDml).where(and(conditions));

				if (operation == Operation.UPDATE) {
					int i = ((Update) mainDml).execute();
					if (i == 0 && hasVersion)
						throw new OptimisticLockException(OPTIMISTIC_LOCK_MSG);

					result = i;
				}
			}

			// processa as operações correspondentes às colunas virtuais
			if (hasVirtual) {
				// since we can have virtual columns pointing to different tables, we need to track each Update
				Map<String, DmlCore<?>>  = new LinkedHashMap<String, DmlCore<?>>();

				// for failed update
				Map<String, Insert> fallbackInsert = null;
				// build all
				for (Column column : table.getColumns()) {
					if (column.isVirtual()) {
						BeanProperty bp = mappings.get(column.getAlias());
						if (bp != null) {
							Object o = null;
							try {
								o = bp.getReadMethod().invoke(bean);
							} catch (Exception e) {
								// e.printStackTrace();
							}

							DmlCore<?> = null;
							Virtual virtual = column.getVirtual();
							Association association = virtual.getAssociation();
							Table tab = association.getTableTo();
							= .get(tab.getAlias());
							if (== null) {
								List<Condition> conds = null;
								switch (operation) {
								case INSERT:
									= createInsert(tab);
									// Set the target key columns used in the association using the origin key values.
									// The target table can have more keys, and those are setted by the discriminator
									for (Relation rel : association.getRelations()) {
										Object val = mainDml.getParameter(rel.getFrom().getColumn());
										set(rel.getTo().getColumn(), val);
									}

									// set discriminator keys
									if (virtual.getDiscriminators() != null) {
										for (Discriminator disc : virtual.getDiscriminators()) {
											value(disc.getColumn(), disc.getValue());
										}
									}
									break;

								case UPDATE:
									if (fallbackInsert == null)
										fallbackInsert = new LinkedHashMap<String, Insert>();
									= createUpdate(tab);
									Insert ins = createInsert(tab);
									fallbackInsert.put(tab.getAlias(), ins);
									conds = new ArrayList<Condition>();
									// Set the target key columns used in the association using the origin key values.
									// The target table can have more keys, and those are setted by the discriminator
									for (Relation rel : association.getRelations()) {
										Object val = mainDml.getParameter(rel.getFrom().getColumn());
										Column col = rel.getTo().getColumn();
										conds.add(col.matches(param(col.getAlias())));
										setParameter(col, val);
										// for fallback insert
										ins.set(col, val);
									}

									// set discriminator keys
									if (virtual.getDiscriminators() != null) {
										for (Discriminator disc : virtual.getDiscriminators()) {
											conds.add(disc.getColumn().matches(disc.getValue()));
											// for fallback insert
											ins.value(disc.getColumn(), disc.getValue());
										}
									}

									((Dml<?>) .where(and(conds));
									break;

								case DELETE:
									= createDelete(tab);
									conds = new ArrayList<Condition>();
									// Set the target key columns used in the association using the origin key values.
									// The target table can have more keys, and those are setted by the discriminator
									for (Relation rel : association.getRelations()) {
										Object val = mainDml.getParameter(rel.getFrom().getColumn());
										Column col = rel.getTo().getColumn();
										conds.add(col.matches(param(col.getAlias())));
										setParameter(col, val);
									}

									// NB: if the main record is deleted, every child should also be deleted, so there is no discriminator condition

									((Dml<?>) .where(and(conds));
									break;
								}

								.put(tab.getAlias(), ;
							}

							if (operation != Operation.DELETE)
								set(virtual.getColumn(), o);

							if (fallbackInsert != null && operation == Operation.UPDATE) {
								Insert ins = fallbackInsert.get(getTable().getAlias());
								if (ins != null)
									ins.set(virtual.getColumn(), o);
							}
						}
					}
				}
				// execute inserts for the virtual tables, if any
				for (Map.Entry<String, DmlCore<?>> entry : .entrySet()) {
					switch (operation) {
					case INSERT:
						((Insert) entry.getValue()).execute();
						break;

					case UPDATE:
						Update upd = (Update) entry.getValue();
						int affected = upd.execute();
						// if no records are affected then a insert needs to be made so that a virtual record is created
						if (affected == 0) {
							Insert ins = fallbackInsert.get(upd.getTable().getAlias());
							ins.execute();
						}
						break;

					case DELETE:
						((Delete) entry.getValue()).execute();
						break;
					}
				}
			}

			// The delete of the main entity is the last one
			if (operation == Operation.DELETE) {
				int i = ((Delete) mainDml).execute();
				if (i == 0 && hasVersion)
					throw new OptimisticLockException(OPTIMISTIC_LOCK_MSG);

				result = i;
			}

			if (versionBeanProperty != null) {
				try {
					versionBeanProperty.getWriteMethod().invoke(bean, versionValue);
				} catch (Exception e) {
					throw new OptimisticLockException(VERSION_SET_MSG, e);
				}
			}
		}

		return result;
	}
}
*/
