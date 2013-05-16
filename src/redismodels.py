#! /usr/bin/env python

# ----------------------------------------------------------------
# @author: Shamail Tayyab
# @date: Thu Apr 4 12:33:00 IST 2013
#
# @desc: Redis/Python ORM for storing relational data in redis.
# ----------------------------------------------------------------

import inspect
import redis

r = redis.Redis ()
r.flushall ()

class classproperty(object):
    """ 
    Lets support for making a property on a class.
    """

    def __init__(self, getter):
        self._getter = getter

    def __get__(self, instance, owner):
        return self._getter(owner)

class RField ():
    """ 
    This class defined a field in Redis Database, similar to a column in a Relational DB.
    """

	# If this field is mandatory.
    required = False                    
	# The default value of this field, if not provided.
    default = None                      

    def __init__ (self, *k, **kw):
        if kw.has_key ("required"):
            self.required = kw['required']
        if kw.has_key ("default"):
            self.default = kw['default']

class StringField (RField):
    """ 
    @inherit RField
    Implementation of String Field, where user wants to store a String in the Database.
    """
    pass

class IntField (RField):
    """ 
    @inherit RField
    Implementation of Integer Field, where user wants to store a Integer in the Database.
    """
    pass

class ForeignKey (RField):
    """ 
    @inherit RField
    Implementation of Foreign Key, where user wants to store One to One relation in the Database.
    """

    def __init__ (self, *k, **kw):
        RField.__init__ (self, *k, **kw)
        self.relation = k[0]

class RModel (object):
    """
    The actual Redis based model class implementation.
    """
    
    id = IntField ()

    keyvals = {}
    locals = []

    def __init__ (self, *k, **kw):
        """ 
        Stores the provided values.
        """
        self.newobj = True
        self.keyvals = {}
        self.locals = []
        self.reinit ()
        for i in self.locals:
            fieldobj = object.__getattribute__(self, i)
            if kw.has_key (i):
                self.keyvals[i] = kw[i]
            else:
                if fieldobj.required == True:
                    if fieldobj.default is not None:
                        self.keyvals[i] = fieldobj.default
                    else:
                        raise Exception ("Need a default value for %s" % (i))

    def from_id (self, id):
        """ 
        Loads a model from its ID.
        """
        self.seq = int(id)
        self.newobj = False
        return self
        
    def reinit (self):
        """
        Reloads the properties of this class from Database.
        """
        #for name, obj in inspect.getmembers (self):
        ##    if isinstance (obj, RField):
        #        self.keyvals[name] = obj.default
        inspect.getmembers (self)

    def validate (self):
        """ 
        Validations for a Field.
        """
        if kw.has_key (name):
            self.keyvals[name] = kw[name]
        elif obj.default is not None:
            self.keyvals[name] = obj.default
        else:
            if obj.required:
                raise AttributeError ("This field is required")

    @property
    def classkey (self):
        """ 
        Generates the Key for this class.
        """
        return 'rmodel:%s' % (self.__class__.__name__.lower ())

    def sequence (self):
        """ 
        Sequence Generator, uses Redis's atomic operation.
        """
        seq_av_at = "%s:__seq__" % (self.classkey)
        seq = r.incr (seq_av_at)
        return seq

    def prepare_key (self, key, for_seq):
        """ 
        Prepares a key to be stored for this class.
        """
        r_key = "%s:%d:%s" % (self.classkey, for_seq, key)
        return r_key

    def save (self):
        """ 
        Persist this object into the Redis Database.
        """
        if self.newobj:
            using_sequence = self.sequence ()
            self.keyvals['id'] = using_sequence
            self.seq = using_sequence
        else:
            using_sequence = self.seq
        for key, val in self.keyvals.items ():
            r_key = self.prepare_key (key, using_sequence)
            r.set (r_key, val)
        self.keyvals = {}
        self.newobj = False

    @classproperty
    def objects (self):
        """ 
        Supports UserClass.objects.all () like stuff.
        """
        return InternalObjectList (self)

    def __getattribute__ (self, attr):
        """ 
        Getter for this class.
        """
        attrib = object.__getattribute__(self, attr)
        if not isinstance (attrib, RField):
            return attrib
        if attr not in self.locals:
            self.locals.append (attr)
        if self.newobj:
            if self.keyvals.has_key (attr):
                return self.keyvals[attr]
            else:
                fieldobj = object.__getattribute__(self, attr)
                return fieldobj.default

        answer = r.get (self.prepare_key (attr, self.seq))
        fieldobj = object.__getattribute__(self, attr)
        if answer == None:
            answer = fieldobj.default
        else:
            if isinstance (fieldobj, ForeignKey):
                fkey = r.get (self.prepare_key ('__relationfor__', self.seq))
                cls = globals ()[fkey]
                return cls.objects.get (id = answer)

        return answer
        
    def __setattr__ (self, attr, val):
        """ 
        Setter for this class.
        """
        try:
            attrib = object.__getattribute__(self, attr)
        except AttributeError:
            object.__setattr__ (self, attr, val)
            return

        if not isinstance (attrib, RField):
            object.__setattr__ (self, attr, val)
            return

        if isinstance (attrib, ForeignKey):
            self.keyvals[attr] = val.id
            self.keyvals['__relationfor__'] = attrib.relation
        else:
            self.keyvals[attr] = val


class InternalObjectList (object):
    """ 
    The query object, to support UserClass.objects.get () or UserClass.object.get_by_id () etc.
    """
    
    def __init__ (self, classfor):
        self.classfor = classfor

    def get_by_id (self, id):
        """ 
        Returns an object by its ID.
        """
        clsfor_obj = self.classfor()
        clsfor_obj.from_id (id)
        return clsfor_obj
        return
        for name, obj in inspect.getmembers (clsfor_obj):
            if isinstance (obj, RField):
                key = clsfor_obj.prepare_key (name, int(id))

    def get (self, *k, **kw):
        """ 
        Returns an object by one of its property, say name.
        """
        if kw.has_key ('id'):
            return self.get_by_id (kw['id'])

if __name__ == "__main__":

    # Lets define a Profile Class which is a Redis Based Model (inherits RModel).
    class Profile (RModel):
        fbid = StringField ()

    # Again, lets define a User.
    class User (RModel):
		# A Field that can store a String.
        username = StringField (required = True)                
        first_name = StringField (required = True)
        last_name = StringField ()
        password = StringField (required = True)
        email = StringField (required = True)

    # Lets now define a Table which will act as foreign key for another table.
    class FK (RModel):
		# Can store a String.
        name = StringField ()                                   

    # Lets now define another Table Test that will have a property for ForeignKey
    class Test (RModel):
        username = StringField ()
		# Stores a String
        password = StringField ()                               
		# Refers to another Table called 'FK'.
        rel = ForeignKey ('FK')                                 
		# Stores a String with some default value.
        defa = StringField (default = 'a')                      
		# Stores a String with some validation.
        req = StringField (required = True, default = 'abc')    

    # Creates an object of FK
    fk = FK (name = 'abc')
    fk.save ()                                                  

    # See if the object is creates?
    print "FKID:", fk.id                                        

    # Lets now create an object for Test
    t = Test (username = "u", password = "p")                   
    # Put the previous object as its relation reference.
    t.rel = fk                                                  
    # Save it.
    t.save ()                                                   
    print t.id

    # See what we get back is the object itself!!
    k= t.rel                                                    
    print "Name:", k.name

    #t.username = "new"
    #t.save ()

    #t = Test ()
    #t.username = 22

    # Lets see what keys were saved in the DB.
    for i in r.keys ():                                         
       print i, r.get (i)

