package com.nannoq.tools.repository.dynamodb.operators;

import com.nannoq.tools.repository.dynamodb.DynamoDBRepository;
import com.nannoq.tools.repository.models.Cacheable;
import com.nannoq.tools.repository.models.DynamoDBModel;
import com.nannoq.tools.repository.models.ETagable;
import com.nannoq.tools.repository.models.Model;
import com.nannoq.tools.repository.repository.Repository;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.lang.reflect.Field;
import java.util.Arrays;

import static com.nannoq.tools.repository.repository.Repository.INCREMENTATION.ADDITION;
import static com.nannoq.tools.repository.repository.Repository.INCREMENTATION.SUBTRACTION;

/**
 * This class defines the update operations for the DynamoDBRepository.
 *
 * @author Anders Mikkelsen
 * @version 17.11.2017
 */
public class DynamoDBUpdater<E extends DynamoDBModel & Model & ETagable & Cacheable> {
    private static final Logger logger = LoggerFactory.getLogger(DynamoDBUpdater.class.getSimpleName());

    private final DynamoDBRepository<E> db;

    public DynamoDBUpdater(DynamoDBRepository<E> db) {
        this.db = db;
    }

    public boolean incrementField(E record, String fieldName) throws IllegalArgumentException {
        Field field = db.checkAndGetField(fieldName);

        try {
            return doCrementation(record, field, ADDITION);
        } catch (IllegalAccessException e) {
            logger.error(e + " : " + e.getMessage() + " : " + Arrays.toString(e.getStackTrace()));

            return false;
        }
    }

    public boolean decrementField(E record, String fieldName) throws IllegalArgumentException {
        Field field = db.checkAndGetField(fieldName);

        try {
            return doCrementation(record, field, SUBTRACTION);
        } catch (IllegalAccessException e) {
            logger.error(e + " : " + e.getMessage() + " : " + Arrays.toString(e.getStackTrace()));

            return false;
        }
    }

    private boolean doCrementation(E record, Field field, Repository.INCREMENTATION direction)
            throws IllegalAccessException {
        if (field.getType() == Long.class) {
            switch (direction) {
                case ADDITION:
                    field.set(record, ((Long) field.get(record)) + 1L);

                    break;
                case SUBTRACTION:
                    if (((Long) field.get(record)) != 0L) {
                        field.set(record, ((Long) field.get(record)) - 1L);
                    }

                    break;
            }

            return true;
        } else if (field.getType() == Integer.class) {
            switch (direction) {
                case ADDITION:
                    field.set(record, ((Integer) field.get(record)) + 1L);

                    break;
                case SUBTRACTION:
                    if (((Integer) field.get(record)) != 0L) {
                        field.set(record, ((Integer) field.get(record)) - 1L);
                    }

                    break;
            }

            return true;
        } else if (field.getType() == Short.class) {
            switch (direction) {
                case ADDITION:
                    field.set(record, ((Short) field.get(record)) + 1L);

                    break;
                case SUBTRACTION:
                    if (((Short) field.get(record)) != 0L) {
                        field.set(record, ((Short) field.get(record)) - 1L);
                    }

                    break;
            }

            return true;
        } else if (field.getType() == Double.class) {
            switch (direction) {
                case ADDITION:
                    field.set(record, ((Double) field.get(record)) + 1L);

                    break;
                case SUBTRACTION:
                    if (((Double) field.get(record)) != 0L) {
                        field.set(record, ((Double) field.get(record)) - 1L);
                    }

                    break;
            }

            return true;
        } else if (field.getType() == Float.class) {
            switch (direction) {
                case ADDITION:
                    field.set(record, ((Float) field.get(record)) + 1L);

                    break;
                case SUBTRACTION:
                    if (((Float) field.get(record)) != 0L) {
                        field.set(record, ((Float) field.get(record)) - 1L);
                    }

                    break;
            }

            return true;
        } else if (field.getType() == long.class) {
            switch (direction) {
                case ADDITION:
                    field.setLong(record, field.getLong(record) + 1L);

                    break;
                case SUBTRACTION:
                    if (field.getLong(record) != 0L) {
                        field.setLong(record, field.getLong(record) - 1L);
                    }

                    break;
            }

            return true;
        } else if (field.getType() == int.class) {
            switch (direction) {
                case ADDITION:
                    field.setInt(record, field.getInt(record) + 1);

                    break;
                case SUBTRACTION:
                    if (field.getInt(record) != 0) {
                        field.setInt(record, field.getInt(record) - 1);
                    }

                    break;
            }

            return true;
        } else if (field.getType() == short.class) {
            short value = field.getShort(record);

            switch (direction) {
                case ADDITION:
                    field.setShort(record, ++value);

                    break;
                case SUBTRACTION:
                    if (value != 0) {
                        field.setShort(record, --value);
                    }

                    break;
            }

            return true;
        } else if (field.getType() == double.class) {
            switch (direction) {
                case ADDITION:
                    field.setDouble(record, field.getDouble(record) + 1.0);

                    break;
                case SUBTRACTION:
                    if (field.getDouble(record) != 0) {
                        field.setDouble(record, field.getDouble(record) - 1.0);
                    }

                    break;
            }

            return true;
        } else if (field.getType() == float.class) {
            switch (direction) {
                case ADDITION:
                    field.setFloat(record, field.getFloat(record) + 1.0F);

                    break;
                case SUBTRACTION:
                    if (field.getFloat(record) != 0) {
                        field.setFloat(record, field.getFloat(record) - 1.0F);
                    }

                    break;
            }

            return true;
        }

        return false;
    }
}
