package org.apache.activemq.artemis.audit;

public interface Auditable<T> {

   T operation() throws Exception;
}
