(in-package :cl-es)

(defvar *redis-location* (list #(127 0 0 1) 6379))

(defun connect-event-source ()
  (redis:connect :host (car *redis-location*)
                 :port (cadr *redis-location*)))

(defun sym-concat (&rest tokens)
  (apply #'concatenate 'string
         (mapcar (lambda (tok)
                   (cond
                     ((stringp tok) (string-upcase tok))
                     ((symbolp tok) (symbol-name tok))
                     (t (error "all tokens must be either string or symbol"))))
                 tokens)))

(defmacro defentity (name members &body body)
  "Defines a business entity with the given name and operations"
  (unless (symbolp name)
    (error "name must be a symbol"))
  (let ((gevent (gensym))
        (gid (gensym)))
    `(defun ,(intern (sym-concat "make-" name)) (&key (getnamep nil))
       (if getnamep
           ,(symbol-name name)
           (let ((,gid nil) ,@members)
             (lambda (,gevent &rest args)
               (declare (ignorable args))
               (if (or (eq ,gevent :create) (eq ,gevent :recreate))
                   (unless (null ,gid)
                     (error "entity already created"))
                   (when (null ,gid)
                     (error "entity not created")))
               (dispatching-ecase ,gevent
                 (:create () (setf ,gid (red:incr (sym-concat ',name "-id"))))
                 (:recreate (id) (setf ,gid id))
                 (:id () ,gid)
                 (:name () ,(symbol-name name))
                 ,@body)))))))

(defmacro dispatching-ecase (keyform &body body)
  (labels ((make-case (case-form)
             (destructuring-bind (test args &rest actions) case-form
               `(,test (destructuring-bind ,args args
                        ,@actions)))))
  `(ecase ,keyform
     ,@(mapcar #'make-case body))))

(defun get-entity-events (entity-name)
  (sym-concat entity-name "-events"))

(defun persist-event (entity event args)
  (let ((key (get-entity-events (funcall entity :name)))
        (id (funcall entity :id)))
    (red:rpush key
               (cl-json:encode-json-to-string
                (list
                 (cons "id" id)
                 (cons "event" event)
                 (cons "args" args))))))

(defun raise-event (entity event &rest args)
  (let ((result (apply entity event args)))
    (persist-event entity event args)
    result))

(defun replay-events (entity-ctor)
  (let* ((key (get-entity-events (funcall entity-ctor :getnamep t)))
         (events (mapcar #'cl-json:decode-json-from-string
                         (red:lrange key 0 -1)))
         (entities (make-hash-table)))
    (dolist (event events entities)
      (let* ((id (cdr (assoc :id event)))
             (ev (values (intern (string-upcase (cdr (assoc :event event)))
                                 "KEYWORD")))
             (args (cdr (assoc :args event)))
             (entity (gethash id entities)))
        (if (null entity)
            (unless (eq ev :create)
              (error "cannot apply event to nonexistant entity with id ~a" id))
            (when (eq ev :create)
              (error "cannot create duplicate entity with id ~a" id)))
        (if (eq ev :create)
            (let ((new (funcall entity-ctor)))
              (funcall new :recreate id)
              (setf (gethash id entities) new))
            (apply entity ev args))))))
